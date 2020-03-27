// command sd-perf-profiler runs configurable perf profiles and uploads
// them to the StackDriver Profiler API in Google Cloud.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"

	cloudprofiler "google.golang.org/genproto/googleapis/devtools/cloudprofiler/v2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

var (
	serverAddr   = flag.String("api", "cloudprofiler.googleapis.com:443", "host:port of cloud profiler API")
	credsJSON    = flag.String("credentials", "", "service account credentials JSON file")
	cloudProject = flag.String("project", "", "Google Cloud project ID")
	service      = flag.String("service", "", "Service name")
	runForever   = flag.Bool("run-forever", false, "Collect profiles indefinitely according to Cloud Profiler's cadence")
)

var (
	requiredScopes = []string{
		"https://www.googleapis.com/auth/monitoring.write",
	}
)

const (
	defaultProfileDuration = time.Second * 5
	maxRequestAttempts     = 10
)

// Currently the best documentation for the agent <-> profiler API protocol
// is in the protobuf service definition, which can be viewed on github here:
//
// https://github.com/googleapis/googleapis/blob/master/google/devtools/cloudprofiler/v2/profiler.proto

type agent struct {
	cloudprofiler.ProfilerServiceClient
	addr    string
	tmpdir  string
	ctx     context.Context
	perf    *exec.Cmd
	service string
	project string
	labels  map[string]string
}

func main() {
	flag.Parse()
	log.Fatal(cloudPerfProfiler())
}

func cloudPerfProfiler() error {
	var creds credentials.PerRPCCredentials
	var err error
	var agent agent

	agent.ctx = context.Background()

	if flag.NArg() > 0 {
		agent.perf = exec.Command("perf", append([]string{"record"}, flag.Args()...)...)
	} else {
		agent.perf = exec.Command("perf", "record", "-ag", "-F", "99", "--", "sleep", "{{ .Duration.Seconds }}")
	}

	if *service != "" {
		agent.service = *service
	} else {
		if service, err := inferService(); err != nil {
			return fmt.Errorf("could not determine service: %s", err)
		} else {
			log.Println("inferring service as", service)
			agent.service = service
		}
	}

	if tmpdir, err := ioutil.TempDir("", filepath.Base(os.Args[0])); err != nil {
		return fmt.Errorf("failed to create temp directory: %s", err)
	} else {
		log.Println("using temporary directory", tmpdir)
		agent.tmpdir = tmpdir
		defer os.RemoveAll(tmpdir)
	}

	if err := os.Chdir(agent.tmpdir); err != nil {
		return err
	}

	if *credsJSON != "" {
		creds, err = oauth.NewServiceAccountFromFile(*credsJSON, requiredScopes...)
		if err != nil {
			return fmt.Errorf("failed to load JSON key: %s", err)
		}
	} else {
		creds, err = oauth.NewApplicationDefault(agent.ctx, requiredScopes...)
		if err != nil {
			return fmt.Errorf("failed to load application default credentials: %s", err)
		}
	}

	log.Println("connecting to", *serverAddr, "...")
	conn, err := grpc.DialContext(agent.ctx, *serverAddr,
		grpc.WithPerRPCCredentials(creds),
		grpc.WithBlock(),
		grpc.WithTransportCredentials(credentials.NewTLS(nil)))
	defer conn.Close()

	if err != nil {
		return fmt.Errorf("error dialing %s: %s", *serverAddr, err)
	}
	agent.addr = conn.Target()
	log.Printf("connected to %s in status %s", conn.Target(), conn.GetState())
	agent.ProfilerServiceClient = cloudprofiler.NewProfilerServiceClient(conn)

	if *cloudProject != "" {
		agent.project = *cloudProject
	} else {
		if project, err := inferCloudProject(creds, conn); err != nil {
			return fmt.Errorf("could not determine project: %s", err)
		} else {
			log.Println("inferred project is", project)
			agent.project = project
		}
	}

	return agent.run()
}

func inferService() (string, error) {
	return os.Hostname()
}

func inferCloudProject(creds credentials.PerRPCCredentials, conn *grpc.ClientConn) (string, error) {
	return "", errors.New("TODO")
}

func (a *agent) run() error {
	for {
		profile, err := a.tryCreateProfile()
		if err != nil {
			return fmt.Errorf("CreateProfile failed: %s", err)
		}
		log.Printf("%s profile requested", profile.ProfileType)
		if err := a.retrieveProfile(profile); err != nil {
			return fmt.Errorf("could not collect perf profile: %s", err)
		}
		if err := a.tryUpdateProfile(profile); err != nil {
			log.Printf("failed to update profile %s: %s", profile.Name, err)
		} else {
			log.Printf("uploaded %s profile %s", profile.ProfileType, profile.Name)
		}
	}
	return nil
}

func (a *agent) tryCreateProfile() (*cloudprofiler.Profile, error) {
	req := &cloudprofiler.CreateProfileRequest{
		Parent: "projects/" + a.project,
		Deployment: &cloudprofiler.Deployment{
			ProjectId: a.project,
			Target:    a.service,
			Labels:    a.labels,
		},
		ProfileType: []cloudprofiler.ProfileType{
			cloudprofiler.ProfileType_CPU,
		},
	}
	md := metadata.New(map[string]string{})

	log.Printf("waiting for profile request from %s", a.addr)

	var (
		attempt int
		backoff time.Duration
		profile *cloudprofiler.Profile
		err     error
	)

	for attempt < maxRequestAttempts {
		profile, err = a.CreateProfile(a.ctx, req, grpc.Trailer(&md))

		if err == nil {
			return profile, nil
		}
		attempt++
		if temporaryError(err) {
			if d, ok := retryError(err, md); ok {
				backoff = d
				log.Printf("CreateProfile failed: %s, retrying using server-advised delay of %v", err, d)
			} else {
				backoff = retryBackoff(attempt)
				log.Printf("CreateProfile failed: %s, retrying in %v", err, backoff)
			}
			time.Sleep(backoff)
		} else {
			return nil, err
		}
	}
	return nil, fmt.Errorf("CreateProfile max retries(%d) exceeded; last error: %s",
		maxRequestAttempts, err)
}

func retryBackoff(attempt int) time.Duration {
	const max = time.Second * 300
	backoff := time.Second
	for i := 0; i < attempt; i++ {
		backoff *= 2
	}
	if backoff > max {
		return max
	}
	return backoff
}

func temporaryError(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch s.Code() {
	case codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Unavailable:
		return true
	}
	return false
}

func retryError(err error, md metadata.MD) (time.Duration, bool) {
	var retryInfo errdetails.RetryInfo

	if s, ok := status.FromError(err); ok && s.Code() == codes.Aborted {
		pb := md.Get("google.rpc.retryinfo-bin")
		if len(pb) > 0 {
			if err := proto.Unmarshal([]byte(pb[0]), &retryInfo); err != nil {
				log.Printf("failed to read retry trailer: %s", err)
			} else {
				d, err := ptypes.Duration(retryInfo.RetryDelay)
				if err != nil {
					log.Printf("could not parse retry delay: %s", err)
				} else {
					return d, true
				}
			}
		}
	}
	return 0, false
}

func (a *agent) retrieveProfile(profile *cloudprofiler.Profile) error {
	if profile.ProfileType != cloudprofiler.ProfileType_CPU {
		return fmt.Errorf("server asked for unsupported profile type %s",
			profile.ProfileType)
	}

	cmd := preparePerfCommand(a.perf, profile)
	timeout, err := ptypes.Duration(profile.Duration)
	if err != nil {
		timeout = defaultProfileDuration
	}
	if err := runPerfCommand(cmd, timeout); err != nil {
		return err
	}
	if err := buildSymbolLookup("binaries", "perf.data"); err != nil {
		return err
	}
	if err := perfToPprof("perf.pprof", "perf.data", "binaries"); err != nil {
		return err
	}
	if pprofBytes, err := ioutil.ReadFile("perf.pprof"); err != nil {
		return err
	} else {
		profile.ProfileBytes = pprofBytes
	}
	return nil

}

func (a *agent) tryUpdateProfile(profile *cloudprofiler.Profile) error {
	req := &cloudprofiler.UpdateProfileRequest{
		Profile: profile,
	}
	_, err := a.UpdateProfile(a.ctx, req)
	return err
}

// Returns copy of cmd with template variables replaced from profile. Cannot be called after cmd is
// running.
func preparePerfCommand(cmd *exec.Cmd, profile *cloudprofiler.Profile) *exec.Cmd {
	var err error
	var params struct {
		*cloudprofiler.Profile
		// Shadow duration with its time.Duration equivalent
		Duration time.Duration
	}
	params.Profile = profile
	params.Duration, err = ptypes.Duration(profile.Duration)
	if err != nil {
		log.Printf("could not parse duration from profile: %s, using default %v", err, defaultProfileDuration)
		params.Duration = defaultProfileDuration
	}

	newCmd := new(exec.Cmd)
	*newCmd = *cmd
	newCmd.Args = append([]string{}, cmd.Args...)

	if len(newCmd.Args) == 0 {
		return newCmd
	}

	var buf bytes.Buffer
	for i, arg := range newCmd.Args {
		t, err := template.New("arg").Parse(arg)
		if err != nil {
			log.Printf("failed to parse arg %q as template: %s", arg, err)
			continue
		}
		buf.Reset()
		if err := t.Execute(&buf, params); err != nil {
			log.Printf("substitute %q failed: %s", arg)
			continue
		}
		newCmd.Args[i] = buf.String()
	}
	return newCmd
}

// Runs perf with a timeout. This is useful if the perf command provided does
// not terminate, for instance if we are profiling a specific process.
func runPerfCommand(cmd *exec.Cmd, timeout time.Duration) error {
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	log.Printf("running %q", cmd.Args)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("Command %q failed: %s; %s", cmd.Args, err)
	}
	time.AfterFunc(timeout, func() {
		if cmd.Process != nil {
			log.Printf("sending INT signal to process %d after %v", cmd.Process.Pid, timeout)
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				log.Printf("interrupt failed: %s", err)
			}
		}
	})

	err := cmd.Wait()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			if exit.ExitCode() == -1 {
				// the process terminated from a signal
				return nil
			} else {
				return fmt.Errorf("Command %q failed: exit status %d; %s",
					cmd.Args, exit.ExitCode(), stderr.String())
			}
		} else {
			return fmt.Errorf("Failed to run perf: %s", err)
		}
	}
	return nil
}

// In order to properly symbolize the resulting pprof proto, perf_to_data
// needs to find the debug symbols. To do this, it searches
// $PPROF_BINARY_PATH. This function constructs a tree of symlinks to help
// pprof find the symbols.
// https://github.com/google/pprof/blob/1ebb73c60ed3b70bd749d4f798d7ae427263e2c5/doc/README.md#annotated-code
func buildSymbolLookup(dst, perfData string) error {
	var n int
	cmd := exec.Command("perf", "buildid-list", perfData)
	output, err := cmd.Output()

	log.Printf("building pprof symbol lookup tree from %s", perfData)
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("perf build-id list failed: %s; %s", err, exit.Stderr)
		}
	}

	for _, line := range strings.Split(string(output), "\n") {
		if len(line) == 0 {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			log.Printf("skipping buildid-list output %q", line)
			continue
		}
		buildid := fields[0]
		symbols := fields[1]
		binary := filepath.Base(fields[1])

		// the kernel symbols are a special case
		if strings.HasPrefix(binary, "vmlinux") {
			binary = "vmlinux"
		}

		if err := os.MkdirAll(filepath.Join(dst, buildid), 0777); err != nil {
			return err
		}

		err := os.Symlink(symbols, filepath.Join(dst, buildid, binary))
		if err != nil && !os.IsExist(err) {
			return err
		}
		n++
	}
	log.Printf("linked debug symbols for %d binaries", n)
	return nil
}

func perfToPprof(dst, src, symbols string) error {
	const maxErrorOutput = 200

	var stderr bytes.Buffer

	// We call pprof instead of calling perf_to_profile because pprof will
	// annotate the profile with symbols.
	cmd := exec.Command("pprof", "-symbolize=force", "-proto", "-output", dst, src)
	cmd.Env = append(cmd.Env,
		"PPROF_BINARY_PATH="+filepath.Join(".", symbols),
		// pprof calls perf_to_profile which must be in path
		os.ExpandEnv("PATH=$PATH"),
	)
	cmd.Stderr = &stderr

	log.Printf("converting %s to pprof format", src)
	if err := cmd.Run(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			errOut := stderr.String()
			if len(errOut) > maxErrorOutput {
				errOut = "... " + errOut[len(errOut)-maxErrorOutput:]
			}
			return fmt.Errorf("Command %q failed: %s; %s", cmd.Args, err, errOut)
		} else {
			return fmt.Errorf("Failed to run  %q: %s", cmd.Args, err)
		}
	}
	return nil
}
