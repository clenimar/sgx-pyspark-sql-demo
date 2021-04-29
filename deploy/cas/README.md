# CAS

> :warning: This is not a production-ready Helm chart and it should be used only for testing or demo purposes. Some parameters and features listed below are not available in this version of the chart. A production-ready CAS chart is available at SconeAppsEE Helm repository.

Deploy the SCONE Configuration and Attestation Service (CAS) to your Kubernetes cluster.

### Prerequisites

* A Kubernetes cluster
* At least one node with the Intel SGX driver installed and available (/dev/isgx or /dev/sgx)

If network policies are enabled (`networkPolicy.enabled=true`), the cluster must use a network plugin that supports NetworkPolicy. Otherwise, network policies will have no effect.

### Install the chart

#### Add the repo

If you haven't yet, please add this repo to Helm.

To deploy CAS with the default parameters to your Kubernetes cluster:

```bash
helm install my-cas sconeappsee/cas
```

**NOTE**: if you are running CAS scone-4.2.1 or older, set `sconeVersion` to the major version of such release. For example, to use CAS scone-4.2.1, `--set sconeVersion=4`.

#### SGX device

By default, this helm chart uses the [SCONE SGX Plugin](../sgxdevplugin). Hence, it sets the resource limits of CAS as follows:

```yaml
resources:
  limits:
    sgx.k8s.io/sgx: 1
```

Alternatively, set `useSGXDevPlugin` to `azure` (e.g., `--useSGXDevPlugin=azure`) to support Azure's SGX Device Plugin. Since Azure requires the amount of EPC memory allocated to your application to be specified, the parameter `sgxEpcMem` (SGX EPC memory in MiB) becomes required too (e.g., `--set useSGXDevPlugin=azure --set sgxEpcMem=16`).

In case you do not want to use the SGX plugin, you can remove the resource limit and explicitly mount the local SGX device into your container by setting:

```yaml
extraVolumes:
  - name: dev-isgx
    hostPath:
      path: /dev/isgx

extraVolumeMounts:
  - name: dev-isgx
    path: /dev/isgx
```

Please note that mounting the local SGX device into your container requires privileged mode, which will grant your container access to ALL host devices. To enable privileged mode, set `securityContext`:

```yaml
securityContext:
  privileged: true
```

### Expose CAS

By default, the CAS instance is only reachable from inside of the cluster (`service.type=ClusterIP`). To expose the CAS instance to outside of the cluster, you can set the parameter `service.type` to either `LoadBalancer` or `NodePort`. If you choose `LoadBalancer` as the type, Kubernetes will expose your service using a managed load balancer and a public IP address (both provisioned by the underlying cloud provider):

```bash
helm install my-cas sconeappsee/cas --set service.type=LoadBalancer
```

If your provider allows user-specified IP addresses for the load balancer, you can specify them via `service.loadBalancerIP`. [In Azure, you have to create a static IP before beforehand](https://docs.microsoft.com/en-us/azure/aks/static-ip). If your provider does not support it, the field will be ignored and an ephemeral IP address will be assigned instead. Please also note that the creation of the resource depends on the availability of the IP address you specify.

```bash
helm install my-cas sconeappsee/cas \
   --set service.type=LoadBalancer \
   --set service.loadBalancerIP=$STATIC_IP
```

If you set `service.type` to `NodePort`, Kubernetes will assign two random ports in the NodePort range that are accessible from outside of the cluster using `$NODEIP:$NODEPORT`. You can specify such ports via `service.nodePorts.enclavePort` and `service.nodePorts.clientPort` (please note that such ports must be available in the nodes, otherwise the resources will not be deployed).

```bash
helm install my-cas sconeappsee/cas \
   --set service.type=NodePort \
   --set service.nodePorts.enclavePort=31000
```

### Run in production mode

By default, this chart installs CAS in debug mode, which is more suitable for development environments. To deploy CAS in production mode, set `productionMode.enabled` to `true`. All the differences between the two modes are listed below.

* Use `productionMode.image` instead of `image` to deploy CAS.

### Network policies

It is possible to limit which pods can contact CAS. If `networkPolicy.enabled=true`, a NetworkPolicy will limit ingress traffic from pods within the same namespace of CAS. To allow pods from other namespaces to contact CAS as well, use `ingressNSMatchLabels` and `ingressPodMatchLabels` to select them. For example, if CAS pods are running in namespace `default`, the following values will allow ingress traffic from ALL pods in `default` namespace, plus ALL pods in namespaces that have the label `project=production`.

```yaml
networkPolicy:
  enabled: true
  ingressNSMatchLabels:
    project: "production"
```

To label a namespace, use `kubectl`:

```bash
kubectl label ns my-namespace project=production
```

To select specific pods in other namespaces, define `ingressPodMatchLabels` too. This way, only pods that match both selectors (namespace AND pod) will be allowed to access CAS.

### Selecting nodes and tolerating taints

To select a set of nodes where CAS should run, you can specify a label through `nodeSelector` parameter. For instance, in order to schedule only on nodes with the label `sgx=true`, use:

```bash
helm install my-cas sconeappsee/cas --set nodeSelector.sgx="true"
```

You can also specify `tolerations` to allow pods to be scheduled on tainted nodes. For instance, to tolerate the taint `sgx=true:NoSchedule`:

```bash
helm install my-cas sconeappsee/cas --set tolerations[0].key=sgx --set tolerations[0].operator=Equal --set tolerations[0].value=true --set tolerations[0].effect=NoSchedule
```

These parameters are useful for heterogeneous clusters, where only a subset of the nodes has the specialized hardware that enables SGX.

### Parameters

|Parameter|Description|Default|
|---|---|---|
`replicaCount`|How many CAS replicas to deploy. Please note in practice that each replica will be **A DIFFERENT CAS** rather than replicas of the same|`1`
`image`|CAS image|`registry.scontain.com:5050/sconecuratedimages/services:cas.preprovisioned`
`imagePullPolicy`|CAS pull policy|`IfNotPresent`
`sconeVersion`|Major version of the SCONE runtime being used. Should be defined if used older runtimes|`5`
`productionMode.enabled`|Deploy CAS is production mode. [Read more](#run-in-production-mode)|`false`
`productionMode.image`|CAS image for production mode|`registry.scontain.com:5050/sconecuratedimages/services:cas`
`imagePullSecrets`|CAS pull secrets, in case of private repositories|`[{"name": "sconeapps"}]`
`securityContext`|Security context for CAS container|`{}`
`service.type`|CAS service type|`ClusterIP`
`service.clientPort`|CAS client port|`8081`
`service.enclavePort`|CAS enclave port|`18765`
`service.loadBalancerIP`|Specify an IP address for your Service, if `service.type=LoadBalancer` and you cloud provider supports it. [Read more](#expose-cas)|`""`
`service.nodePorts.clientPort`|Specify a nodePort for the CAS client port, if `service.type=NodePort`. [Read more](#expose-cas)|`""`
`service.nodePorts.enclavePort`|Specify a nodePort for the CAS enclave port, if `service.type=NodePort`. [Read more](#expose-cas)|`""`
`livenessProbe.enabled`|Whether the CAS health checks are enabled or not|`false`
`livenessProbe.attestedCLI`|If set to `true`, run liveness probes with an attested SCONE CLI (this also ensures that attestation is working properly)|`true`
`livenessProbe.periodSeconds`|How often to run the liveness probe, in seconds|`60`
`livenessProbe.failureThreshold`|How many failures in a row to tolerate before restarting the pod|`3`
`env`|Additional environment variables for CAS container|`[{"name": "SCONE_LAS_ADDR", "valueFrom": {"fieldRef": {"fieldPath": "status.hostIP"}}}]`
`configuration`|Configuration to override cas.toml file|`nil`
`networkPolicy.enabled`|Limit CAS ingress traffic to the same namespace|`false`
`networkPolicy.ingressNSMatchLabels`|Selectors to allow ingress traffic to other namespaces|`{}`
`networkPolicy.ingressPodMatchLabels`|Selectors to allow ingress traffic to certain pods in other namespaces|`{}`
`persistence.enabled`|If set to `true`, create a PVC claim alongside CAS|`false`
`persistence.mountPath`|Where to mount CAS persistent volume|`/etc/cas/db`
`persistence.storageClass`|Define a storageClassName for the provisioned PersistentVolumeClaim. If unset or set to nil (default), use default storageClassName. If set to "-", disable dynamic provisioning|`nil`
`persistence.annotations`|PersistentVolumeClaim annotations|`""`
`persistence.accessModes`|PersistentVolume access modes|`["ReadWriteOnce"]`
`persistence.size`|PersistentVolume size|`1Gi`
`resources`|CPU/Memory resource requests/limits for node.|`{}`
`nodeSelector`|Node labels for pod assignment (this value is evaluated as a template)|`{}`
`tolerations`|List of node taints to tolerate (this value is evaluated as a template)|`[]`
`affinity`|Map of node/pod affinities (The value is evaluated as a template)|`{}`
`extraVolumes`|Extra volume definitions|`[]`
`extraVolumeMounts`|Extra volume mounts for CAS pod|`[]`
`useSGXDevPlugin`|Use [SGX Device Plugin](../sgxdevplugin) to access SGX resources.|`"scone"`
`sgxEpcMem`|Required to Azure SGX Device Plugin. Protected EPC memory in MiB|`nil`
