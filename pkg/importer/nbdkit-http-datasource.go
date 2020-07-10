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

package importer

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"

	"k8s.io/klog"

	cdiv1 "kubevirt.io/containerized-data-importer/pkg/apis/core/v1beta1"
	"kubevirt.io/containerized-data-importer/pkg/image"
	"kubevirt.io/containerized-data-importer/pkg/util"
)

var nbdkitOperations = image.NewNBDKitOperations()

// NbdkitHTTPDataSource the data source for nbdkit with http backed data
type NbdkitHTTPDataSource struct {
	// For info call to determine format.
	httpReader io.ReadCloser
	ctx        context.Context
	cancel     context.CancelFunc
	cancelLock sync.Mutex

	// content type expected by the to live on the endpoint.
	contentType cdiv1.DataVolumeContentType
	// stack of readers
	readers *FormatReaders
	// endpoint the http endpoint to retrieve the data from.
	endpoint *url.URL
	// url the url to report to the caller of getURL, could be the endpoint, or a file in scratch space.
	url *url.URL
	// true if we are using a custom CA (and thus have to use scratch storage)
	customCA bool

	certDir string
	// the content length reported by the http server.
	contentLength uint64
}

// NewNbdkitHTTPDataSource creates a new instance of the nbdkit http data provider.
func NewNbdkitHTTPDataSource(endpoint, accessKey, secKey, certDir string, contentType cdiv1.DataVolumeContentType) (*NbdkitHTTPDataSource, error) {
	ep, err := ParseEndpoint(endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, fmt.Sprintf("unable to parse endpoint %q", endpoint))
	}
	ctx, cancel := context.WithCancel(context.Background())
	httpReader, contentLength, _, err := createHTTPReader(ctx, ep, accessKey, secKey, certDir)
	if err != nil {
		cancel()
		return nil, err
	}

	if accessKey != "" && secKey != "" {
		ep.User = url.UserPassword(accessKey, secKey)
	}
	httpSource := &NbdkitHTTPDataSource{
		ctx:           ctx,
		cancel:        cancel,
		httpReader:    httpReader,
		contentType:   contentType,
		endpoint:      ep,
		customCA:      certDir != "",
		contentLength: contentLength,
		certDir:       certDir,
	}
	return httpSource, nil
}

// Info is called to get initial information about the data.
func (hs *NbdkitHTTPDataSource) Info() (ProcessingPhase, error) {
	var err error
	hs.readers, err = NewFormatReaders(hs.httpReader, hs.contentLength)
	if hs.contentType != cdiv1.DataVolumeKubeVirt {
		return ProcessingPhaseError, errors.New("This data source only supports kubevirt disk images")
	}
	if err != nil {
		klog.Errorf("Error creating readers: %v", err)
		return ProcessingPhaseError, err
	}
	hs.url = hs.endpoint
	return ProcessingPhaseTransferDataFile, nil
}

// Transfer is called to transfer the data from the source to a scratch location.
func (hs *NbdkitHTTPDataSource) Transfer(path string) (ProcessingPhase, error) {
	size, err := util.GetAvailableSpace(path)
	if size <= int64(0) {
		//Path provided is invalid.
		return ProcessingPhaseError, ErrInvalidPath
	}
	file := filepath.Join(path, tempFile)
	args := &image.NBDKitArgs{
		SourceURL: hs.endpoint,
		Dest:      file,
	}
	err = nbdkitOperations.ConvertAndWrite(args)
	if err != nil {
		return ProcessingPhaseError, err
	}
	return ProcessingPhaseResize, nil
}

// TransferFile is called to transfer the data from the source to the passed in file.
func (hs *NbdkitHTTPDataSource) TransferFile(fileName string) (ProcessingPhase, error) {
	args := &image.NBDKitArgs{
		SourceURL: hs.endpoint,
		Dest:      fileName,
	}
	if hs.readers.Xz {
		args.Filters = append(args.Filters, image.Xz)
	}
	if hs.customCA {
		args.CertDir = hs.certDir
	}
	err := nbdkitOperations.ConvertAndWrite(args)
	if err != nil {
		return ProcessingPhaseError, err
	}
	return ProcessingPhaseResize, nil
}

// Process is called to do any special processing before giving the URI to the data back to the processor
func (hs *NbdkitHTTPDataSource) Process() (ProcessingPhase, error) {
	return ProcessingPhaseConvert, nil
}

// GetURL returns the URI that the data processor can use when converting the data.
func (hs *NbdkitHTTPDataSource) GetURL() *url.URL {
	return hs.url
}

// Close all readers.
func (hs *NbdkitHTTPDataSource) Close() error {
	var err error
	if hs.readers != nil {
		err = hs.readers.Close()
	}
	hs.cancelLock.Lock()
	if hs.cancel != nil {
		hs.cancel()
		hs.cancel = nil
	}
	hs.cancelLock.Unlock()
	return err
}
