package node

import (
	"context"
	"k8s.io/kubernetes/pkg/kubelet/kuberuntime"
	"time"

	g "github.com/onsi/ginkgo/v2"
	o "github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	admissionapi "k8s.io/pod-security-admission/api"

	exutil "github.com/openshift/origin/test/extended/util"
)

var _ = g.Describe("[sig-node] [FeatureGate:ImageVolume] ImageVolume", func() {
	defer g.GinkgoRecover()

	f := framework.NewDefaultFramework("image-volume")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	var (
		oc      = exutil.NewCLI("image-volume")
		podName = "image-volume-test"
		image   = "image-registry.openshift-image-registry.svc:5000/openshift/cli:latest"
	)

	//g.BeforeEach(func() {
	//	// Skip if ImageVolume feature is not enabled
	//	if !exutil.IsTechPreviewNoUpgrade(context.TODO(), oc.AdminConfigClient()) {
	//		g.Skip("skipping, this feature is only supported on TechPreviewNoUpgrade clusters")
	//	}
	//})

	g.It("should succeed with pod and pull policy of Always", func(ctx context.Context) {
		g.By("Creating a pod with image volume")
		pod := buildPodWithImageVolume(f.Namespace.Name, "", podName, image,
			"ls", "/mnt/image/bin/oc",
		)
		_, err := oc.AdminKubeClient().CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		o.Expect(err).NotTo(o.HaveOccurred())

		g.By("Waiting for pod to succeed")
		err = e2epod.WaitForPodSuccessInNamespace(ctx, oc.AdminKubeClient(), pod.Name, pod.Namespace)
		o.Expect(err).NotTo(o.HaveOccurred())

		log, err := oc.AdminKubeClient().CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &v1.PodLogOptions{}).DoRaw(ctx)
		o.Expect(err).NotTo(o.HaveOccurred())
		o.Expect(log).NotTo(o.BeEmpty())
	})

	g.It("should handle multiple image volumes", func(ctx context.Context) {
		g.By("Creating a pod with multiple image volumes")
		pod := buildPodWithMultipleImageVolumes(f.Namespace.Name, "", podName,
			image,
			image,
			"sh", "-c", "ls /mnt/image/bin/oc && ls /mnt/image2/bin/oc",
		)
		_, err := oc.AdminKubeClient().CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		o.Expect(err).NotTo(o.HaveOccurred())

		g.By("Waiting for pod to succeed")
		err = e2epod.WaitForPodSuccessInNamespace(ctx, oc.AdminKubeClient(), pod.Name, pod.Namespace)
		o.Expect(err).NotTo(o.HaveOccurred())

		log, err := oc.AdminKubeClient().CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &v1.PodLogOptions{}).DoRaw(ctx)
		o.Expect(err).NotTo(o.HaveOccurred())
		o.Expect(log).NotTo(o.BeEmpty())
	})

	g.It("should fail when image does not exist", func(ctx context.Context) {
		g.By("Creating a pod with non-existent image volume")
		pod := buildPodWithImageVolume(f.Namespace.Name, "", podName, "nonexistent:latest")
		_, err := oc.AdminKubeClient().CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		o.Expect(err).NotTo(o.HaveOccurred())

		g.By("Waiting for pod to be ImagePullBackOff")
		err = e2epod.WaitForPodCondition(ctx, oc.AdminKubeClient(), pod.Namespace, pod.Name, "ImagePullBackOff", 60*time.Second, func(pod *v1.Pod) (bool, error) {
			return len(pod.Status.ContainerStatuses) > 0 &&
				pod.Status.ContainerStatuses[0].State.Waiting != nil &&
				pod.Status.ContainerStatuses[0].State.Waiting.Reason == "ImagePullBackOff", nil
		})
		o.Expect(err).NotTo(o.HaveOccurred())
	})

	g.It("should succeed if image volume is not existing but unused", func(ctx context.Context) {
		g.By("Creating a pod with non-existent image volume")
		pod := buildPodWithImageVolume(f.Namespace.Name, "", podName, "nonexistent:latest", "pwd")
		pod.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{}
		_, err := oc.AdminKubeClient().CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		o.Expect(err).NotTo(o.HaveOccurred())

		g.By("Waiting for pod to succeed")
		err = e2epod.WaitForPodSuccessInNamespace(ctx, oc.AdminKubeClient(), pod.Name, pod.Namespace)
		o.Expect(err).NotTo(o.HaveOccurred())
	})

	g.It("should succeed with multiple pods and same image on the same node", func(ctx context.Context) {
		g.By("Creating pod1 with image volume")
		pod1 := buildPodWithImageVolume(f.Namespace.Name, "", podName, image,
			"sh", "-c", "trap 'exit 0' TERM INT; sleep infinity & wait",
		)
		_, err := oc.AdminKubeClient().CoreV1().Pods(f.Namespace.Name).Create(ctx, pod1, metav1.CreateOptions{})
		o.Expect(err).NotTo(o.HaveOccurred())

		g.By("Waiting for pod1 to be running")
		err = e2epod.WaitForPodRunningInNamespace(ctx, oc.AdminKubeClient(), pod1)
		o.Expect(err).NotTo(o.HaveOccurred())

		pod1, err = oc.AdminKubeClient().CoreV1().Pods(f.Namespace.Name).Get(ctx, pod1.Name, metav1.GetOptions{})

		g.By("Creating pod2 with image volume")
		pod2 := buildPodWithImageVolume(f.Namespace.Name, pod1.Spec.NodeName, podName+"-2", image,
			"sh", "-c", "trap 'exit 0' TERM INT; sleep infinity & wait",
		)
		_, err = oc.AdminKubeClient().CoreV1().Pods(f.Namespace.Name).Create(ctx, pod2, metav1.CreateOptions{})
		o.Expect(err).NotTo(o.HaveOccurred())

		g.By("Waiting for pod2 to be running")
		err = e2epod.WaitForPodRunningInNamespace(ctx, oc.AdminKubeClient(), pod2)
		o.Expect(err).NotTo(o.HaveOccurred())

		g.By("Verifying image volume in pod1 is mounted")
		stdout := e2epod.ExecCommandInContainer(f, pod1.Name, pod1.Spec.Containers[0].Name,
			"ls", "/mnt/image/bin/oc",
		)
		o.Expect(stdout).NotTo(o.BeEmpty())

		g.By("Verifying image volume in pod2 is mounted")
		stdout = e2epod.ExecCommandInContainer(f, pod2.Name, pod2.Spec.Containers[0].Name,
			"ls", "/mnt/image/bin/oc",
		)
		o.Expect(stdout).NotTo(o.BeEmpty())
	})

	g.Context("when subPath is used", func() {
		g.It("should handle image volume with subPath", func(ctx context.Context) {
			g.By("Creating a pod with image volume and subPath")
			pod := buildPodWithImageVolumeSubPath(f.Namespace.Name, "", podName, image, "bin",
				"ls", "/mnt/image/oc",
			)
			_, err := oc.AdminKubeClient().CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
			o.Expect(err).NotTo(o.HaveOccurred())

			g.By("Waiting for pod to succeed")
			err = e2epod.WaitForPodSuccessInNamespace(ctx, oc.AdminKubeClient(), pod.Name, pod.Namespace)
			o.Expect(err).NotTo(o.HaveOccurred())

			log, err := oc.AdminKubeClient().CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &v1.PodLogOptions{}).DoRaw(ctx)
			o.Expect(err).NotTo(o.HaveOccurred())
			o.Expect(log).NotTo(o.BeEmpty())
		})

		g.It("should fail to mount image volume with invalid subPath", func(ctx context.Context) {
			g.By("Creating a pod with image volume and subPath")
			pod := buildPodWithImageVolumeSubPath(f.Namespace.Name, "", podName, image, "noexist",
				"pwd",
			)
			_, err := oc.AdminKubeClient().CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
			o.Expect(err).NotTo(o.HaveOccurred())

			err = e2epod.WaitForPodContainerToFail(ctx, oc.AdminKubeClient(), pod.Namespace, pod.Name, 0, kuberuntime.ErrCreateContainer.Error(), 60*time.Second)
			o.Expect(err).NotTo(o.HaveOccurred())
		})
	})
})

func buildPodWithImageVolume(namespace, nodeName, podName, image string, commands ...string) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Name:    "test-container",
					Image:   "image-registry.openshift-image-registry.svc:5000/openshift/tools:latest",
					Command: commands,
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "image-vol",
							MountPath: "/mnt/image",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "image-vol",
					VolumeSource: v1.VolumeSource{
						Image: &v1.ImageVolumeSource{
							Reference: image,
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
	return pod
}

func buildPodWithImageVolumeSubPath(namespace, nodeName, podName, image, subPath string, commands ...string) *v1.Pod {
	pod := buildPodWithImageVolume(namespace, nodeName, podName, image, commands...)
	pod.Spec.Containers[0].VolumeMounts[0].SubPath = subPath
	return pod
}

func buildPodWithMultipleImageVolumes(namespace, nodeName, podName, image1, image2 string, commands ...string) *v1.Pod {
	pod := buildPodWithImageVolume(namespace, nodeName, podName, image1, commands...)
	pod.Spec.Volumes = append(pod.Spec.Volumes, v1.Volume{
		Name: "image-vol-2",
		VolumeSource: v1.VolumeSource{
			Image: &v1.ImageVolumeSource{
				Reference: image2,
			},
		},
	})
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, v1.VolumeMount{
		Name:      "image-vol-2",
		MountPath: "/mnt/image2",
	})
	return pod
}
