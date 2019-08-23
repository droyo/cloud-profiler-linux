// command sd-perf-profiler runs configurable perf profiles and uploads
// them to the StackDriver Profiler API in Google Cloud.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"os"
	"os/exec"
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
	serverAddr     = flag.String("api", "cloudprofiler.googleapis.com:443", "host:port of cloud profiler API")
	credsJSON      = flag.String("credentials-json", "", "service account credentials JSON file")
	cloudProject   = flag.String("project", "", "Google Cloud project ID")
	service        = flag.String("service", "", "Service name")
	enablePerfMem  = flag.Bool("enable-perf-mem", false, "enable memory profiles with `perf mem`")
	enablePerfLock = flag.Bool("enable-perf-lock", false, "enable lock profiles with `perf lock`")
	onerun         = flag.String("onerun", false, "upload a profile immediately and exit.")
)

var (
	requiredScopes = []string{
		"https://www.googleapis.com/auth/monitoring.write",
	}
)

const (
	defaultProfileDuration = time.Second * 10
)

// Currently the best documentation for the agent <-> profiler API protocol
// is in the protobuf service definition, which can be viewed on github here:
//
// https://github.com/googleapis/googleapis/blob/master/google/devtools/cloudprofiler/v2/profiler.proto

type agent struct {
	cloudprofiler.ProfilerServiceClient
	ctx           context.Context
	cpuProfile    *exec.Cmd
	memProfile    *exec.Cmd
	lockProfile   *exec.Cmd
	threadProfile *exec.Cmd
	service       string
	project       string
	labels        map[string]string
}

func main() {
	flag.Parse()
	var creds credentials.PerRPCCredentials
	var err error

	ctx := context.Background()

	if *credsJSON != "" {
		creds, err = oauth.NewServiceAccountFromFile(*credsJSON, requiredScopes...)
		if err != nil {
			log.Fatal("failed to load JSON key: ", err)
		}
	} else {
		creds, err = oauth.NewApplicationDefault(ctx, requiredScopes...)
		if err != nil {
			log.Fatal("failed to load application default credentials: ", err)
		}
	}

	log.Println("connecting to ", *serverAddr, "...")
	conn, err := grpc.DialContext(ctx, *serverAddr,
		grpc.WithPerRPCCredentials(creds),
		grpc.WithBlock(),
		grpc.WithTransportCredentials(credentials.NewTLS(nil)))
	defer conn.Close()

	if err != nil {
		log.Fatalf("error dialing %s: %s", *serverAddr, err)
	}
	log.Printf("connected to %s in status %s", conn.Target(), conn.GetState())
	agent := agent{
		ProfilerServiceClient: cloudprofiler.NewProfilerServiceClient(conn),
		ctx: ctx,
	}
	if *enablePerfMem {
		agent.memProfile = exec.Command("perf", "mem", "record", "sleep", "{{ .Duration.Seconds }}")
	}
	if *enablePerfLock {
		agent.lockProfile = exec.Command("perf", "lock", "record", "sleep", "{{ .Duration.Seconds }}")
	}
	if flag.NArg() > 0 {
		agent.cpuProfile = exec.Command(flag.Arg(0), flag.Args()[1:]...)
	} else {
		agent.cpuProfile = exec.Command("perf", "record", "-ag", "-F", "99", "sleep", "{{ .Duration.Seconds }}")
	}
	if *cloudProject != "" {
		agent.project = *cloudProject
	} else {
		if project, err := inferCloudProject(creds, conn); err != nil {
			log.Fatal("could not determine project: ", err)
		} else {
			agent.project = project
		}
	}

	if *service != "" {
		agent.service = *service
	} else {
		if service, err := inferService(); err != nil {
			log.Fatal("could not determine service: ", err)
		} else {
			agent.service = service
		}
	}
	log.Fatal(agent.run())
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
		if err := a.retrieveProfile(profile); err != nil {
			log.Printf("could not retrieve profile: %s", err)
			continue
		}
		if err := a.tryUpdateProfile(profile); err != nil {
			log.Printf("failed to update profile %s: %s", profile.Name, err)
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
		ProfileType: a.supportedProfileTypes(),
	}
	md := metadata.New(map[string]string{})

	log.Printf("retrieving profile lease from %s", *serverAddr)

	var (
		attempt int
		backoff time.Duration
	)

	for {
		profile, err := a.CreateProfile(a.ctx, req, grpc.Trailer(&md))

		if err == nil {
			return profile, nil
		}
		attempt++
		if temporaryError(err) {
			if d, ok := retryError(err); ok {
				backoff = d
				log.Println("CreateProfile failed: %s, retrying using server-advised delay of %v", d)
			} else {
				backoff = retryBackoff(attempt)
				log.Printf("CreateProfile failed: %s, retrying in %v", err, backoff)
			}
			time.Sleep(backoff)
		} else {
			return nil, err
		}
	}
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

func retryError(err error) (time.Duration, bool) {
	var retryInfo errdetails.RetryInfo
	if s, ok := status.FromError(err); ok && s.Code() == codes.Aborted {
		pb := md.Get("google.rpc.retryinfo-bin")
		if len(pb) > 0 {
			if err := proto.Unmarshal(pb[0], &retryInfo); err != nil {
				log.Printf("failed to read retry trailer: %s", err)
			} else {
				d, err := ptypes.Duration(retryInfo.RetryDelay)
				if err != nil {
					log.Printf("could not parse retry delay: %s", err)
				} else {
					backoff = d
					return d, true
				}
			}
		}
	}
	return 0, false
}

func (a *agent) retrieveProfile(profile *cloudprofiler.Profile) error {
	var perfCmd *exec.Cmd

	switch profile.ProfileType {
	case cloudprofiler.ProfileType_CPU:
		if a.cpuProfile != nil {
			perfCmd = a.cpuProfile
		}
	case cloudprofiler.ProfileType_HEAP:
		if a.memProfile != nil {
			perfCmd = a.memProfile
		}
	case cloudprofiler.ProfileType_CONTENTION:
		if a.lockProfile != nil {
			perfCmd = a.lockProfile
		}
	case cloudprofiler.ProfileType_THREADS:
		if a.threadProfile != nil {
			perfCmd = a.threadProfile
		}
	}
	if perfCmd != nil {
		return perfToProfile(profile, perfCmd)
	}
	return fmt.Errorf("unsupported profile type %s", profile.ProfileType)
}

func (a *agent) tryUpdateProfile(profile *cloudprofiler.Profile) error {
	req := &cloudprofiler.UpdateProfileRequest{
		Profile: profile,
	}
	_, err := a.UpdateProfile(a.ctx, req)
	return err
}

func (a *agent) supportedProfileTypes() (supported []cloudprofiler.ProfileType) {
	if a.cpuProfile != nil {
		append(supported, cloudprofiler.ProfileType_CPU)
	}
	if a.threadProfile != nil {
		append(supported, cloudprofiler.ProfileType_THREAD)
	}
	if a.memProfile != nil {
		append(supported, cloudprofiler.ProfileType_HEAP)
	}
	if a.lockProfile != nil {
		append(supported, cloudprofiler.ProfileType_CONTENTION)
	}
	return supported
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
	params.Duration, err = ptypes.Duration(profile.Profile.Duration)
	if err != nil {
		log.Printf("could not parse duration from profile: %s, using default %v", err, defaultProfileDuration)
		params.Duration = defaultProfileDuration
	}

	t := template.New("arg")
	newCmd := new(exec.Cmd)

	*newCmd = *cmd
	newCmd.Args = append([]string, cmd.Args...)

	if len(newCmd.Args) == 0 {
		return newCmd
	}

	var buf bytes.Buffer
	for i, arg := range newCmd.Args {
		t, err := t.Parse(arg)
		if err != nil {
			continue
		}
		buf.Reset()
		if err := t.Exec(&buf, params); err != nil {
			continue
		}
		newCmd.Args[i] = buf.String()
	}
	return newCmd
}

func perfToProfile(profile *cloudprofiler.Profile, cmd *exec.Cmd) error {
	cmd = preparePerfCommand(cmd, profile)
	timeout, err := ptypes.Duration(profile.Duration)
	if err != nil {
		timeout = defaultProfileDuration
	}
	if err := runPerfCommand(cmd, timeout); err != nil {
		return err
	}
	pprofBytes, err := perfToPprofGzip("perf.data")
	if err != nil {
		return err
	}
	profile.ProfileBytes = pprofBytes
	return nil
}

func runPerfCommand(cmd *exec.Cmd, timeout time.Duration) error {
	time.AfterFunc(timeout, func() {
		if cmd.Process != nil {
			log.Printf("sending INT signal to process %d after %v", cmd.Process.Pid, timeout)
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				log.Printf("interrupt failed: %s", err)
			}
		}
	})

	log.Printf("running %q", cmd.Args)
	err := cmd.Run()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			// perf will exit with status code 130 if it is interrupted
			if exit.ExitCode() == 130 {
				return nil
			} else {
				return fmt.Errorf("Command %q failed: %s; %s", cmd.Args, err, string(exit.Stderr))
			}
		} else {
			return fmt.Errorf("Failed to run perf: %s", err)
		}
	}
	return nil
}

func perfToPprofGzip(src string) ([]byte, error) {
	var buf bytes.Buffer
	var gzipErr error

	gzipCompleted := make(chan struct{})

	cmd := exec.Command("perf_to_profile", "-i", src, "-o", "/dev/stdout")
	r, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	w := gzip.NewWriter(&buf)

	go func() {
		_, gzipErr = io.Copy(&profileBuffer)
		w.Close()
		close(gzipCompleted)
	}()

	if err := cmd.Wait(); err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("Command %q failed: %s; %s", cmd.Args, err, string(exit.Stderr))
		} else {
			return nil, fmt.Errorf("Failed to run  %q: %s", cmd.Args, err)
		}
	}
	<-gzipCompleted
	if gzipErr != nil {
		return nil, fmt.Errorf("gzip of pprof data failed: %s", gzipErr)
	}
	return buf.Bytes(), nil
}
