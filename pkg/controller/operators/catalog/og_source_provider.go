package catalog

import (
	"context"
	"fmt"

	v1listers "github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/listers/operators/v1"

	"github.com/operator-framework/operator-lifecycle-manager/pkg/controller/registry/resolver/cache"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/labels"
)

type OperatorGroupToggleSourceProvider struct {
	sp              SourceProviderWithInvalidate
	logger          *logrus.Logger
	ogLister        v1listers.OperatorGroupLister
	globalNamespace string
}

func NewOperatorGroupToggleSourceProvider(sp SourceProviderWithInvalidate, logger *logrus.Logger,
	ogLister v1listers.OperatorGroupLister, globalNamespace string) *OperatorGroupToggleSourceProvider {
	return &OperatorGroupToggleSourceProvider{
		sp:              sp,
		logger:          logger,
		ogLister:        ogLister,
		globalNamespace: globalNamespace,
	}
}

// SourceProviderWithInvalidate is a client-side interface for the OperatorGroup Toggle
type SourceProviderWithInvalidate interface {
	cache.SourceProvider
	Invalidate(key cache.SourceKey)
}

const exclusionAnnotation string = "olm.operatorframework.io/exclude-global-namespace-resolution"

func (e *OperatorGroupToggleSourceProvider) Sources(namespaces ...string) map[cache.SourceKey]cache.Source {
	// Check if annotation is set first
	resolutionNamespaces, err := e.CheckForExclusion(namespaces...)
	if err != nil {
		// Fail early with a dummy Source that returns an error
		// Note: this only errors in the case the Lister fails, which should not happen
		// TODO: Update the Sources interface to return an error
		m := make(map[cache.SourceKey]cache.Source)
		key := cache.SourceKey{Name: "og-exclusion-failure", Namespace: namespaces[0]}
		source := errorSource{err}
		m[key] = source
		return m
	}
	return e.sp.Sources(resolutionNamespaces...)
}

func (e *OperatorGroupToggleSourceProvider) Invalidate(key cache.SourceKey) {
	e.sp.Invalidate(key)
}

type errorSource struct {
	error
}

func (e errorSource) Snapshot(ctx context.Context) (*cache.Snapshot, error) {
	return nil, e.error
}

func (e *OperatorGroupToggleSourceProvider) CheckForExclusion(namespaces ...string) ([]string, error) {
	var defaultResult = namespaces
	// The first namespace provided is always the current namespace being synced
	var ownNamespace = namespaces[0]
	var toggledResult = []string{ownNamespace}

	if ownNamespace == e.globalNamespace {
		// Global namespace is being synced
		// Return early with default
		return defaultResult, nil
	}

	// Check the OG on the NS provided for the exclusion annotation
	ogs, err := e.ogLister.OperatorGroups(ownNamespace).List(labels.Everything())
	if err != nil {
		// Assume resolution was global in the case of an error (the default behavior)
		return defaultResult, fmt.Errorf("listing operatorgroups in namespace %s: %s", ownNamespace, err)
	}

	if len(ogs) != 1 || ogs[0] == nil {
		// Problem with the operatorgroup configuration in the namespace
		// The namespace may not be configured as an operator installation target namespace
		// Assume global resolution (default)
		return defaultResult, nil
	}

	var og = ogs[0]
	if og.Annotations == nil {
		// No exclusion specified
		return defaultResult, nil
	}
	if val, ok := og.Annotations[exclusionAnnotation]; ok && val == "true" {
		// Exclusion specified
		// Ignore the globalNamespace for the purposes of resolution in this namespace
		e.logger.Printf("excluding global catalogs from resolution in namespace %s", ownNamespace)
		return toggledResult, nil
	}

	return defaultResult, nil
}
