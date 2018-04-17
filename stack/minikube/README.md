ZooKeeper/Kafka stack on Kubernetes
===

# Getting started
The examples here assume that the deployment is on [Minikube](https://kubernetes.io/docs/getting-started-guides/minikube/). All scripts should be run from the current directory. Behind the scenes, these scripts will use either `kubectl` and `minikube`.

## Preparing the environment
Start Minikube: `minikube start`

One deployed, Kafka will be made accessible on `kafka.minikube:30092`. Edit `/etc/hosts` on the host, adding the following entry:

```
192.168.99.100  kafka.minikube
```

Replace `192.168.99.100` with the result of running `minikube ip`.

## Starting and stopping the ZK/Kafka stack
To create the ZooKeeper & Kafka resources and bring up the deployment:

```
./kube-up
```

To bring down the deployment and delete the resources:

```
./kube-down
```

## Viewing the stack parameters
To describe the current cluster (deployments, services, pods and volumes):

```
./kube-list
```

The following will list the service URLs as they are bound on the Minikube VM:

```
./minikube-urls
```

In the example above, ZooKeeper is hosted at `192.168.99.100:30181` while Kafka is hosted at `192.168.99.100:30092`.

To connect to the pod instance, run `kubectl exec -it <pod> bash`.

# Regenerate Kubernetes resource files
To regenerate the resource files with [Kompose](http://kompose.io) (if `docker-compose.yaml` was edited):

```sh
rm *-deployment.yaml 
rm *-service.yaml 
kompose convert
```

Then edit each `*-service.yaml`, adding a `nodePort` parameter. Also change ZooKeeper's `port` and `targetPort` to `2181`. Examples below:

**`zookeeper-service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  annotations:
    kompose.cmd: kompose convert
    kompose.service.type: NodePort
    kompose.version: 1.11.0 ()
  creationTimestamp: null
  labels:
    io.kompose.service: zookeeper
  name: zookeeper
spec:
  ports:
  - name: "2181"
    port: 2181
    targetPort: 2181
    nodePort: 30181
  selector:
    io.kompose.service: zookeeper
  type: NodePort
status:
  loadBalancer: {}
```

**`kafka-service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  annotations:
    kompose.cmd: kompose convert
    kompose.service.type: NodePort
    kompose.version: 1.11.0 ()
  creationTimestamp: null
  labels:
    io.kompose.service: kafka
  name: kafka
spec:
  ports:
  - name: "30092"
    port: 30092
    targetPort: 30092
    nodePort: 30092
  selector:
    io.kompose.service: kafka
  type: NodePort
status:
  loadBalancer: {}
```

Edit `kafka-deployment.yaml` and set `hostAliases` so that `kafka.minikube` can be resolved locally to `localhost`.

**`kafka-deployment.yaml`**

```yaml
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  annotations:
    kompose.cmd: kompose convert
    kompose.service.type: NodePort
    kompose.version: 1.11.0 ()
  creationTimestamp: null
  labels:
    io.kompose.service: kafka
  name: kafka
spec:
  replicas: 1
  strategy: {}
  template:
    metadata:
      creationTimestamp: null
      labels:
        io.kompose.service: kafka
    spec:
      hostAliases:
      - ip: "127.0.0.1"
        hostnames:
        - "kafka.minikube"
      containers:
      - env:
        - name: KAFKA_ADVERTISED_HOST_NAME
          value: "kafka.minikube"
        - name: KAFKA_ADVERTISED_PORT
          value: "30092"
        - name: KAFKA_PORT
          value: "30092"
        - name: KAFKA_ZOOKEEPER_CONNECT
          value: zookeeper:2181
        image: wurstmeister/kafka
        name: kafka
        ports:
        - containerPort: 30092
        resources: {}
      restartPolicy: Always
status: {}
```