# LAS

Deploy the SCONE Local Attestation Service (LAS) to your Kubernetes cluster.

### Prerequisites

* A Kubernetes cluster;
* At least one node with the Intel SGX driver installed and available (/dev/isgx).

### Install the chart

#### Add the repo

If you haven't yet, please add this repo to Helm.

To deploy LAS with the default parameters to your Kubernetes cluster:

```bash
helm install my-las sconeapps/las
```

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

### Selecting nodes and tolerating taints

To select a set of nodes where LAS should run, you can specify a label through `nodeSelector` parameter. For instance, in order to schedule only on nodes with the label `sgx=true`, use:

```bash
helm install my-las sconeapps/las --set nodeSelector.sgx="true"
```

You can also specify `tolerations` to allow pods to be scheduled on tainted nodes. For instance, to tolerate the taint `sgx=true:NoSchedule`:

```bash
helm install my-las sconeapps/las --set tolerations[0].key=sgx --set tolerations[0].operator=Equal --set tolerations[0].value=true --set tolerations[0].effect=NoSchedule
```

These parameters are useful for heterogeneous clusters, where only a subset of the nodes has the specialized hardware that enables SGX.

### Parameters

|Parameter|Description|Default|
|---|---|---|
`image`|LAS image|`registry.scontain.com:5050/sconecuratedimages/services:las`
`imagePullPolicy`|LAS pull policy|`IfNotPresent`
`imagePullSecrets`|LAS pull secrets, in case of private repositories|`[{"name": "sconeapps"}]`
`service.enabled`|Whether to expose the LAS daemons with a Kubernetes service or not|`false`
`service.type`|LAS service type|`ClusterIP`
`service.port`|LAS service port|`18766`
`service.hostPort`|Whether to expose the LAS service port on the host network or not|`false`
`securityContext`|Security context for LAS container|`{}`
`resources`|CPU/Memory resource requests/limits for node.|`{}`
`nodeSelector`|Node labels for pod assignment (this value is evaluated as a template)|`{}`
`tolerations`|List of node taints to tolerate (this value is evaluated as a template)|`[]`
`extraVolumes`|Extra volume definitions|`[]`
`extraVolumeMounts`|Extra volume mounts for LAS pod|`[]`
`useSGXDevPlugin`|Use [SGX Device Plugin](../sgxdevplugin) to access SGX resources.|`"scone"`
`sgxEpcMem`|Required to Azure SGX Device Plugin. Protected EPC memory in MiB|`nil`
`externalAesmd.enabled`|Use an external AESM instead of creating one|`false``
`externalAesmd.path`|Path to the aesmd socket on the host|`/var/run/aesmd`
