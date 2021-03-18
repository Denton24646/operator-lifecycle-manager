package finalizers

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stretchr/testify/require"

	"github.com/operator-framework/api/pkg/operators/v1alpha1"
)

func TestUpdateCleanupFinalizer(t *testing.T) {
	CSV := func(cleanupEnabled bool, addFinalizer bool, phase v1alpha1.ClusterServiceVersionPhase) *v1alpha1.ClusterServiceVersion {
		csv := &v1alpha1.ClusterServiceVersion{
			ObjectMeta: metav1.ObjectMeta{
				Finalizers: []string{},
			},
			Spec: v1alpha1.ClusterServiceVersionSpec{
				Cleanup: v1alpha1.CleanupSpec{
					Enabled: cleanupEnabled,
				},
			},
			Status: v1alpha1.ClusterServiceVersionStatus{
				Phase: phase,
			},
		}

		if addFinalizer {
			csv.Finalizers = append(csv.Finalizers, CleanupFinalizer)
		}
		return csv
	}

	testCases := []struct {
		Name        string
		InputCSV    *v1alpha1.ClusterServiceVersion
		ExpectedCSV *v1alpha1.ClusterServiceVersion
	}{
		{
			Name:        "Attach cleanup finalizer when cleanup is enabled",
			InputCSV:    CSV(true, false, v1alpha1.CSVPhaseNone),
			ExpectedCSV: CSV(true, true, v1alpha1.CSVPhaseNone),
		},
		{
			Name:        "Remove cleanup finalizer when cleanup is disabled",
			InputCSV:    CSV(false, true, v1alpha1.CSVPhaseNone),
			ExpectedCSV: CSV(false, false, v1alpha1.CSVPhaseNone),
		},
		{
			Name:        "No update/Return nil: Cleanup enabled and finalizer already present",
			InputCSV:    CSV(true, true, v1alpha1.CSVPhaseNone),
			ExpectedCSV: nil,
		},
		{
			Name:        "No update/Return nil: Cleanup disabled and finalizer not present",
			InputCSV:    CSV(false, false, v1alpha1.CSVPhaseNone),
			ExpectedCSV: nil,
		},
		{
			Name:        "Force cleanup opt-out: CSV is in Replacing phase with cleanup enabled and finalizer present",
			InputCSV:    CSV(true, true, v1alpha1.CSVPhaseReplacing),
			ExpectedCSV: CSV(false, false, v1alpha1.CSVPhaseReplacing),
		},
		{
			Name:        "Force cleanup opt-out: CSV is in Replacing phase with cleanup disabled and finalizer present",
			InputCSV:    CSV(false, true, v1alpha1.CSVPhaseReplacing),
			ExpectedCSV: CSV(false, false, v1alpha1.CSVPhaseReplacing),
		},
		{
			Name:        "Force cleanup opt-out: CSV is in Replacing phase with cleanup enabled and finalizer not present",
			InputCSV:    CSV(true, false, v1alpha1.CSVPhaseReplacing),
			ExpectedCSV: CSV(false, false, v1alpha1.CSVPhaseReplacing),
		},
		{
			Name:        "No update when CSV is in Replacing phase with cleanup disabled and finalizer not present",
			InputCSV:    CSV(false, false, v1alpha1.CSVPhaseReplacing),
			ExpectedCSV: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			outputCSV := updateCleanupFinalizer(tc.InputCSV)
			require.Equal(t, tc.ExpectedCSV, outputCSV)
		})
	}
}
