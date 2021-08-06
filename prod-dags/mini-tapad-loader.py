import json
from pathlib import Path
import yaml
import datetime


from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.utils.timezone import TIMEZONE

CONFIG_FILE = "mini-tapad-loader.yaml"
def get_volume_mounts(volume_mount_def):
  return VolumeMount(
    volume_mount_def["name"],
    volume_mount_def["mount_path"],
    volume_mount_def.get("sub_path"),
    volume_mount_def.get("read_only") or False
  )
def get_volumes(volume_def):
  return Volume(
    volume_def["name"],
    volume_def["configs"]
  )
def create_secret(secret_def):
    return Secret(
      "env",
      secret_def["env"],
      secret_def["name"],
      secret_def["key"]
    )
def create_secret_volume(secret_def):
    return Secret(
      "volume",
      secret_def["mount_path"],
      secret_def["secret"]
    )
def get_env(env_def):
  a = {}
  a.update({env_def["name"]: env_def["value"]})
  return a
def build_env_dict(envs):
  a = {}
  for env in envs:
    a = {**a, **env}
  return a
def get_label_conf_exec(label_def):
  return (
    ["--conf"] +
    [
      "spark.kubernetes.executor.label.{}={}".format(
        label_def["name"], label_def["value"]
      )
    ]
  )
def get_label_conf_driver(label_def):
  return (
    ["--conf"] +
    [
      "spark.kubernetes.driver.label.{}={}".format(
        label_def["name"], label_def["value"]
      )
    ]
  )
def get_spark_secret_envs_driver(secret_def):
  return (
    ["--conf"] +
    [
      "spark.kubernetes.driver.secretKeyRef.{}={}:{}".format(
        secret_def["env_name"],
        secret_def["secret_name"],
        secret_def["key_name"]
      )
    ]
  )
def get_spark_secret_envs_executor(secret_def):
  return (
    ["--conf"] +
    [
      "spark.kubernetes.executor.secretKeyRef.{}={}:{}".format(
        secret_def["env_name"],
        secret_def["secret_name"],
        secret_def["key_name"]
      )
    ]
  )
def get_spark_secret_driver(secret_def):
  return (
    ["--conf"] +
    [
      "spark.kubernetes.driver.secrets.{}={}".format(
        secret_def["name"],
        secret_def["path"]
      )
    ]
  )
def get_spark_secret_executor(secret_def):
  return (
    ["--conf"] +
    [
      "spark.kubernetes.executor.secrets.{}={}".format(
        secret_def["name"],
        secret_def["path"]
      )
    ]
  )
def get_spark_driver_env(env_def):
  return (
    ["--conf"] +
    [
      "spark.kubernetes.driverEnv.{}={}".format(
        env_def["name"],
        env_def["value"]
      )
    ]
  )
def get_spark_executor_env(env_def):
  return (
    ["--conf"] +
    [
      "spark.kubernetes.executorEnv.{}={}".format(
        env_def["name"],
        env_def["value"]
      )
    ]
  )
def get_custom_config(config_def):
  return (
    ["--conf"] +
    [
      "spark.{}={}".format(
        config_def["name"],
        config_def["value"]
      )
    ]
  )
def get_spark_node_selector(node_selector_def):
  return (
    ["--conf"] +
    [
      "spark.kubernetes.node.selector.{}={}".format(
        node_selector_def["name"],
        node_selector_def["value"]
      )
    ]
  )
def get_extra_driver_class_path(extra_paths):
  if (len(extra_paths) == 0):
    return ""
  else:
    return "spark.driver.extraClassPath={}".format(
      ':'.join(extra_paths)
    )
def get_extra_executor_class_path(extra_paths):
  if (len(extra_paths) == 0):
    return ""
  else:
    return "spark.executor.extraClassPath={}".format(
      ':'.join(extra_paths)
    )
def get_key(key, step_config):
  if key in step_config:
    return step_config[key]
  else:
    return list()
def flatten(l):
  return [item for sublist in l for item in sublist]
def get_spark_kube_pod_operator(step_config, dag, config):
    driver_class_path = get_extra_driver_class_path(
      get_key("extra_class_paths", step_config["spark"]["driver"])
    )
    executor_class_path = get_extra_executor_class_path(
      get_key("extra_class_paths", step_config["spark"]["executor"])
    )
    return dict(
      name=step_config["name"],
      depends_on=get_key("depends_on", step_config),
      operator=KubernetesPodOperator(
        task_id='{}_{}'.format(config["dag"]["dag_id"], step_config["name"]),
        dag=dag,
        namespace="default",
        name='{}-{}'.format(config["dag"]["dag_id"], step_config["name"]),
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=False,
        image=step_config["spark_submit_image"],
        cmds=[
          "/opt/spark/bin/spark-submit"
        ],
        arguments=(
          [
            "--master", step_config["spark"]["master"],
            "--class", step_config["spark"]["entry_class"],
            "--deploy-mode", "cluster",
            "--name", '{}-{}'.format(config["dag"]["dag_id"], step_config["name"]),
            "--conf", "spark.kubernetes.container.image.pullPolicy=Always",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
            "--conf", "spark.kubernetes.driver.container.image={}".format(step_config["spark"]["driver"]["image"]),
            "--conf", "spark.driver.memory={}".format(step_config["spark"]["driver"]["memory"]),
            "--conf", "spark.kubernetes.executor.container.image={}".format(step_config["spark"]["executor"]["image"]),
            "--conf", "spark.executor.instances={}".format(step_config["spark"]["executor"]["instances"]),
            "--conf", "spark.executor.memory={}".format(step_config["spark"]["executor"]["memory"]),
            "--conf", "spark.executor.cores={}".format(step_config["spark"]["executor"]["cores"]),
          ]
          + list(flatten(map(get_label_conf_exec, get_key("labels", step_config["spark"]["executor"]))))
          + list(flatten(map(get_label_conf_driver, get_key("labels", step_config["spark"]["driver"]))))
          + list(flatten(map(get_spark_secret_envs_driver, get_key("env_secrets", step_config["spark"]["driver"]))))
          + list(flatten(map(get_spark_secret_envs_executor, get_key("env_secrets", step_config["spark"]["executor"]))))
          + list(flatten(map(get_spark_secret_driver, get_key("secrets", step_config["spark"]["driver"]))))
          + list(flatten(map(get_spark_secret_executor, get_key("secrets", step_config["spark"]["executor"]))))
          + list(flatten(map(get_spark_driver_env, get_key("env_vars", step_config["spark"]["driver"]))))
          + list(flatten(map(get_spark_executor_env, get_key("env_vars", step_config["spark"]["executor"]))))
          + list(flatten(map(get_spark_node_selector, get_key("node_selectors", step_config["spark"]))))
          + list(flatten(map(get_custom_config, get_key("custom_configs", step_config["spark"]))))
          + (
              list() if len(driver_class_path) == 0 else ["--conf", driver_class_path]
            )
          + (
              list() if len(executor_class_path) == 0 else ["--conf", executor_class_path]
            )
          + [step_config["spark"]["application"]]
          + list(get_key("args", step_config["spark"]))
        )
      )
    )
def get_kubernetes_pod_operator(step_config, dag, config):
    return dict(
      name=step_config["name"],
      depends_on=get_key("depends_on", step_config),
      operator=KubernetesPodOperator(
        task_id='{}_{}'.format(config["dag"]["dag_id"], step_config["name"]),
        dag=dag,
        namespace="default",
        name='{}-{}'.format(config["dag"]["dag_id"], step_config["name"]),
        volume_mounts=list(map(get_volume_mounts, get_key("volume_mounts", step_config))),
        volumes=list(map(get_volumes, get_key("volumes", step_config))),
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        node_selectors=build_env_dict(list(map(get_env, get_key("node_selectors", step_config)))),
        image=step_config["image"],
        image_pull_policy="Always",
        cmds=list(get_key("cmds", step_config)),
        env_vars=build_env_dict(list(map(get_env, get_key("env_vars", step_config)))),
        arguments=list(get_key("arguments", step_config)),
        secrets=(
          list(map(create_secret, get_key("secrets", step_config))) + 
          list(map(create_secret_volume, get_key("secret_volumes", step_config)))
        )
      )
    )
def get_operator(step_config, dag, config):
  if step_config["type"] == "scala-spark":
    return get_spark_kube_pod_operator(step_config, dag, config)
  elif step_config["type"] == "base-kube":
    return get_kubernetes_pod_operator(step_config, dag, config)
# Load the DAG configuration, setting a default if none is present
path = Path(__file__).with_name(CONFIG_FILE)
with path.open() as f:
    default_config = yaml.safe_load(f)
config = json.loads(json.dumps(default_config, default=str))  # Serialize datetimes to strings
# Create the DAG
parsed_dag = config["dag"]
parsed_dag["start_date"] = datetime.datetime.strptime(
  parsed_dag["start_date"], '%Y-%m-%dT%H:%M:%S'
).replace(
  tzinfo=TIMEZONE
)
with DAG(**parsed_dag) as dag:
    operator_list = [get_operator(step, dag, config) for step in config["steps"]]
    # Setup the dependencies
    for operator in operator_list:
      if "depends_on" in operator:
        for dep in operator["depends_on"]:
          for operator_sub in operator_list:
            if dep == operator_sub["name"]:
              operator_sub["operator"] >> operator["operator"]
      else:
        operator["operator"]