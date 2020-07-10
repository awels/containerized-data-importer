/*
Copyright 2018 The CDI Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package image

import (
	"fmt"
	"net/url"
	"os"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
	"kubevirt.io/containerized-data-importer/pkg/common"
	"kubevirt.io/containerized-data-importer/pkg/system"
	"kubevirt.io/containerized-data-importer/pkg/util"
)

const (
	// Xz The XZ filter type
	Xz NBDKitFilter = "xz"
	// Gz The Gzip plugin type
	Gz NBDKitPlugin = "gz"
)

// NBDKitFilter Filter type for NBDkit
type NBDKitFilter string

// NBDKitPlugin Plugin type for NBDkit
type NBDKitPlugin string

// NBDKitArgs The arguments to pass to nbdkit
type NBDKitArgs struct {
	SourceURL *url.URL
	Dest      string
	Filters   []NBDKitFilter
	Plugins   []NBDKitPlugin
	CertDir   string
	AccessKey string
	SecKey    string
}

// NBDKitOperations Are the operations available to call nbdkit
type NBDKitOperations interface {
	// Use ndbkit to convert and write the image to the Destination
	ConvertAndWrite(args *NBDKitArgs) error
}

type nbdkitOperations struct{}

var (
	nbdkitExecFunction = system.ExecWithLimits
	nbdkitIterface     = NewNBDKitOperations()
)

func init() {
	if err := prometheus.Register(progress); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			// A counter for that metric has been registered before.
			// Use the old counter from now on.
			progress = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			klog.Errorf("Unable to create prometheus progress counter")
		}
	}
	ownerUID, _ = util.ParseEnvVar(common.OwnerUID, false)
}

// NewNBDKitOperations returns the default implementation of QEMUOperations
func NewNBDKitOperations() NBDKitOperations {
	return &nbdkitOperations{}
}

func (o *nbdkitOperations) appendCurlArgs(commandArgs []string, args *NBDKitArgs) []string {
	// Readonly since we don't want to write.
	commandArgs = append(commandArgs, "-r")
	commandArgs = append(commandArgs, "curl")
	commandArgs = append(commandArgs, "--verbose")
	if args.CertDir != "" {
		commandArgs = append(commandArgs, fmt.Sprintf("cainfo=%s/%s", args.CertDir, "tls.crt"))
	}
	commandArgs = append(commandArgs, args.SourceURL.String())
	return commandArgs
}

func (o *nbdkitOperations) appendRunArgs(commandArgs []string, args *NBDKitArgs) []string {
	commandArgs = append(commandArgs, "--run")
	commandArgs = append(commandArgs, fmt.Sprintf("/usr/bin/qemu-img convert -p $nbd -t none -O raw %s", args.Dest))
	return commandArgs
}

func (o *nbdkitOperations) ConvertAndWrite(args *NBDKitArgs) error {

	// nbdkit -U - curl https://download.fedoraproject.org/pub/fedora/linux/releases/32/Cloud/x86_64/images/Fedora-Cloud-Base-32-1.6.x86_64.qcow2 \
	// --run 'qemu-img convert -p $nbd -O raw Fedora.raw'

	//	jsonArg := fmt.Sprintf("json: {\"file.driver\": \"%s\", \"file.url\": \"%s\", \"file.timeout\": %d}", args.SourceURL.Scheme, args.SourceURL, networkTimeoutSecs)

	commandArgs := make([]string, 0)

	commandArgs = append(commandArgs, "-U")
	commandArgs = append(commandArgs, "-")
	commandArgs = o.appendCurlArgs(commandArgs, args)
	for _, filter := range args.Filters {
		commandArgs = append(commandArgs, fmt.Sprintf("--filter=%s", filter))
	}
	commandArgs = o.appendRunArgs(commandArgs, args)

	_, err := nbdkitExecFunction(nil, reportProgress, "/usr/sbin/nbdkit", commandArgs...)
	if err != nil {
		// TODO: Determine what to do here, the conversion failed, and we need to clean up the mess, but we could be writing to a block device
		os.Remove(args.Dest)
		return errors.Wrap(err, "could not stream/convert image to raw")
	}

	return nil
}
