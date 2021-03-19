package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/operator-framework/operator-lifecycle-manager/pkg/controller/operators/olm/finalizers"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/ghodss/yaml"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/clientset/versioned"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/lib/operatorclient"
	"github.com/operator-framework/operator-lifecycle-manager/test/e2e/ctx"
)

const (
	PlumResource = "plums"
	PlumGroup    = "plumbus.operators.io"
	PlumName     = PlumResource + "." + PlumGroup
	PlumKind     = "Plum"
	PlumVersion  = "v1"
	PlumCR1Name  = "my-plum-1"
	PlumCSV1Name = "csv-plum-1"
)

var _ = Describe("operand deletion", func() {
	// a ClusterServiceVersions owns a cluster-scoped CustomResourceDefinition
	//   a second ClusterServiceVersion owns the same CustomResourceDefinition
	//   a second ClusterServiceVersion requires the same CustomResourceDefinition
	// a ClusterServiceVersions owns a namespace-scoped CustomResourceDefinition
	//   no associated OperatorGroup exists
	//   multiple associated OperatorGroups exist
	//   an incorrect target namespaces annotation is set on the ClusterServiceVersion
	//   a second ClusterServiceVersion owns the same CustomResourceDefinition (both in and out of target namespace set)
	//   a second ClusterServiceVersion requires the same CustomResourceDefinition (both in and out of target namespace set)
	// a CR deletes that take a long time, ensure that the CSV is resynced and deletion proceeds

	var (
		kubeClient     operatorclient.ClientInterface
		operatorClient versioned.Interface
		dynamicClient  dynamic.Interface
		plumGVR        schema.GroupVersionResource
	)

	BeforeEach(func() {
		kubeClient = ctx.Ctx().KubeClient()
		operatorClient = ctx.Ctx().OperatorClient()
		dynamicClient = ctx.Ctx().DynamicClient()
		plumGVR = schema.GroupVersionResource{
			Group:    PlumGroup,
			Version:  PlumVersion,
			Resource: PlumResource,
		}
	})

	// Setup Fixtures is used across specs and is responsible for setting up and tearing down test fixtures that
	// include CRDs, CSVs, and CRs. First the CRD is created, then the CSV that owns the CRD, then the dummy CR within
	// a BeforeEach(). These are all then removed in an AfterEach() in an idempotent way.
	SetupFixtures := func(crds []*apiextensionsv1.CustomResourceDefinition, csvs []*v1alpha1.ClusterServiceVersion, crs []*unstructured.Unstructured) {
		BeforeEach(func() {
			// create crds
			for _, crd := range crds {
				Eventually(func() error {
					_, err := kubeClient.ApiextensionsInterface().ApiextensionsV1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{})
					if err != nil {
						if !k8serrors.IsAlreadyExists(err) {
							return err
						}
					}
					return nil
				}).Should(Succeed())
			}
			// ensure crd is established and accepted on the cluster before continuing
			for _, crd := range crds {
				Eventually(func() (bool, error) {
					crd, err := kubeClient.ApiextensionsInterface().ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), crd.GetName(), metav1.GetOptions{})
					if err != nil {
						return false, err
					}
					return CRDready(&crd.Status), nil
				}).Should(BeTrue())
			}

			// create csvs
			for _, csv := range csvs {
				Eventually(func() error {
					_, err := operatorClient.OperatorsV1alpha1().ClusterServiceVersions(csv.GetNamespace()).Create(context.TODO(), csv, metav1.CreateOptions{})
					if err != nil {
						if !k8serrors.IsAlreadyExists(err) {
							return err
						}
					}
					return nil
				}).Should(Succeed())
			}
			// wait for csvs to succeed
			for _, csv := range csvs {
				_, err := fetchCSV(operatorClient, csv.GetName(), csv.GetNamespace(), csvSucceededChecker)
				Expect(err).ShouldNot(HaveOccurred())
			}

			// create crs (cluster scoped)
			for _, cr := range crs {
				Eventually(func() error {
					_, err := dynamicClient.Resource(plumGVR).Create(context.TODO(), cr, metav1.CreateOptions{})
					if err != nil {
						if !k8serrors.IsAlreadyExists(err) {
							return err
						}
					}
					return nil
				}).Should(Succeed())
			}
		})

		AfterEach(func() {
			// delete csv
			for _, csv := range csvs {
				Eventually(func() (bool, error) {
					err := operatorClient.OperatorsV1alpha1().ClusterServiceVersions(csv.GetNamespace()).Delete(context.TODO(), csv.GetName(), metav1.DeleteOptions{})
					if k8serrors.IsNotFound(err) {
						return true, nil
					}
					return false, err
				}).Should(BeTrue())
			}

			// delete the crs
			for _, cr := range crs {
				Eventually(func() (bool, error) {
					err := dynamicClient.Resource(plumGVR).Delete(context.TODO(), cr.GetName(), metav1.DeleteOptions{})
					if k8serrors.IsNotFound(err) {
						return true, nil
					}
					return false, err
				}).Should(BeTrue())
			}

			// delete crd
			for _, crd := range crds {
				Eventually(func() (bool, error) {
					err := kubeClient.ApiextensionsInterface().ApiextensionsV1().CustomResourceDefinitions().Delete(context.TODO(), crd.GetName(), metav1.DeleteOptions{})
					if k8serrors.IsNotFound(err) {
						return true, nil
					}
					return false, err
				}).Should(BeTrue())
			}
		})
	}

	Context("Given a cluster-scoped CSV that owns a cluster scoped CRD", func() {
		ns := "operators" // TODO
		When("the CSV has cleanup disabled on the spec and is deleted", func() {
			SetupFixtures(
				[]*apiextensionsv1.CustomResourceDefinition{plumbusCRD(apiextensionsv1.ClusterScoped)},
				[]*v1alpha1.ClusterServiceVersion{plumCSV(PlumCSV1Name, ns, false, nil)},
				[]*unstructured.Unstructured{unmarshalCR("./testdata/plum/plumCR1.yaml")})

			It("should not delete the associated CRs", func() {
				err := operatorClient.OperatorsV1alpha1().ClusterServiceVersions(ns).Delete(context.TODO(), PlumCSV1Name, metav1.DeleteOptions{})
				Expect(err).ToNot(HaveOccurred())

				Eventually(func() (bool, error) {
					_, err := operatorClient.OperatorsV1alpha1().ClusterServiceVersions(ns).Get(context.TODO(), PlumCSV1Name, metav1.GetOptions{})
					if k8serrors.IsNotFound(err) {
						return true, nil
					}
					return false, err
				}).Should(BeTrue())

				// Ensure the CR is on-cluster
				Eventually(func() error {
					_, err := dynamicClient.Resource(plumGVR).Get(context.TODO(), PlumCR1Name, metav1.GetOptions{})
					if k8serrors.IsNotFound(err) {
						return fmt.Errorf("could not find operand when deletion was not specified")
					}
					return err
				}).Should(Succeed())
			})
		})

		When("the CSV has cleanup enabled on the spec but no cleanup finalizer and is deleted", func() {
			SetupFixtures(
				[]*apiextensionsv1.CustomResourceDefinition{plumbusCRD(apiextensionsv1.ClusterScoped)},
				[]*v1alpha1.ClusterServiceVersion{plumCSV(PlumCSV1Name, ns, true, nil)},
				[]*unstructured.Unstructured{unmarshalCR("./testdata/plum/plumCR1.yaml")})

			It("should not delete the associated CRs", func() {
				err := operatorClient.OperatorsV1alpha1().ClusterServiceVersions(ns).Delete(context.TODO(), PlumCSV1Name, metav1.DeleteOptions{})
				Expect(err).ToNot(HaveOccurred())

				Eventually(func() (bool, error) {
					_, err := operatorClient.OperatorsV1alpha1().ClusterServiceVersions(ns).Get(context.TODO(), PlumCSV1Name, metav1.GetOptions{})
					if k8serrors.IsNotFound(err) {
						return true, nil
					}
					return false, err
				}).Should(BeTrue())

				// Ensure the CR is on-cluster
				Eventually(func() error {
					_, err := dynamicClient.Resource(plumGVR).Get(context.TODO(), PlumCR1Name, metav1.GetOptions{})
					if k8serrors.IsNotFound(err) {
						return fmt.Errorf("could not find operand when deletion was not specified")
					}
					return nil
				}).Should(Succeed())
			})
		})

		// Base case scenario: operand deletion cleans up operands
		When("the CSV has cleanup enabled via its spec and the finalizer and is deleted", func() {
			SetupFixtures(
				[]*apiextensionsv1.CustomResourceDefinition{plumbusCRD(apiextensionsv1.ClusterScoped)},
				[]*v1alpha1.ClusterServiceVersion{plumCSV(PlumCSV1Name, ns, true, []string{finalizers.FinalizerDeleteCustomResources})},
				[]*unstructured.Unstructured{unmarshalCR("./testdata/plum/plumCR1.yaml")})

			It("should delete the associated CRs", func() {
				// Ensure the CSV is deleted
				err := operatorClient.OperatorsV1alpha1().ClusterServiceVersions(ns).Delete(context.TODO(), PlumCSV1Name, metav1.DeleteOptions{})
				Expect(err).ToNot(HaveOccurred())

				Eventually(func() (bool, error) {
					_, err := operatorClient.OperatorsV1alpha1().ClusterServiceVersions(ns).Get(context.TODO(), PlumCSV1Name, metav1.GetOptions{})
					if k8serrors.IsNotFound(err) {
						return true, nil
					}
					return false, err
				}).Should(BeTrue())

				// Ensure the CR count is zero
				Eventually(func() error {
					_, err := dynamicClient.Resource(plumGVR).Get(context.TODO(), PlumCR1Name, metav1.GetOptions{})
					if k8serrors.IsNotFound(err) {
						return nil
					}
					return fmt.Errorf("waiting for operand deletion")
				}).Should(Succeed())
			})
		})
	})
})

func plumCSV(name, namespace string, cleanup bool, finalizers []string) *v1alpha1.ClusterServiceVersion {
	return &v1alpha1.ClusterServiceVersion{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.ClusterServiceVersionKind,
			APIVersion: v1alpha1.ClusterServiceVersionAPIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			Finalizers: finalizers,
		},
		Spec: v1alpha1.ClusterServiceVersionSpec{
			InstallModes: []v1alpha1.InstallMode{
				{
					Type:      v1alpha1.InstallModeTypeOwnNamespace,
					Supported: true,
				},
				{
					Type:      v1alpha1.InstallModeTypeSingleNamespace,
					Supported: true,
				},
				{
					Type:      v1alpha1.InstallModeTypeMultiNamespace,
					Supported: true,
				},
				{
					Type:      v1alpha1.InstallModeTypeAllNamespaces,
					Supported: true,
				},
			},
			InstallStrategy: v1alpha1.NamedInstallStrategy{
				StrategyName: v1alpha1.InstallStrategyNameDeployment,
				StrategySpec: v1alpha1.StrategyDetailsDeployment{
					DeploymentSpecs:    []v1alpha1.StrategyDeploymentSpec{},
					Permissions:        []v1alpha1.StrategyDeploymentPermissions{},
					ClusterPermissions: []v1alpha1.StrategyDeploymentPermissions{},
				},
			},
			Cleanup: v1alpha1.CleanupSpec{
				Enabled: cleanup,
			},
			CustomResourceDefinitions: v1alpha1.CustomResourceDefinitions{
				Owned: []v1alpha1.CRDDescription{
					{
						Name:    PlumName,
						Version: PlumVersion,
						Kind:    PlumKind,
					},
				},
			},
		},
	}
}

func plumbusCRD(scope apiextensionsv1.ResourceScope) *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: PlumName,
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: PlumGroup,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    PlumVersion,
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type:        "object",
							Description: "my crd schema",
						},
					},
				},
			},
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   PlumResource,
				Singular: strings.ToLower(PlumKind),
				Kind:     PlumKind,
				ListKind: "PlumList",
			},
			Scope: scope,
		},
	}
}

func CRDready(status *apiextensionsv1.CustomResourceDefinitionStatus) bool {
	if status == nil {
		return false
	}
	established, namesAccepted := false, false
	for _, cdt := range status.Conditions {
		switch cdt.Type {
		case apiextensionsv1.Established:
			if cdt.Status == apiextensionsv1.ConditionTrue {
				established = true
			}
		case apiextensionsv1.NamesAccepted:
			if cdt.Status == apiextensionsv1.ConditionTrue {
				namesAccepted = true
			}
		}
	}
	return established && namesAccepted
}

func unmarshalCR(path string) *unstructured.Unstructured {
	// create CR
	cr := unstructured.Unstructured{}
	data, err := ioutil.ReadFile(path)
	Expect(err).ToNot(HaveOccurred())
	y, err := yaml.YAMLToJSON(data)
	Expect(err).ToNot(HaveOccurred())
	err = json.Unmarshal(y, &cr)
	Expect(err).ToNot(HaveOccurred())
	return &cr
}
