{{- define "msg-queue.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "msg-queue.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "msg-queue.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "msg-queue.name" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* etcd fullname: <release>-etcd unless overridden */}}
{{- define "msg-queue.etcdFullname" -}}
{{- if .Values.etcd.fullnameOverride -}}
{{- .Values.etcd.fullnameOverride -}}
{{- else -}}
{{- printf "%s-etcd" .Release.Name -}}
{{- end -}}
{{- end -}}

{{/* etcd headless service: <etcdFullname>-headless */}}
{{- define "msg-queue.etcdHeadlessService" -}}
{{- printf "%s-headless" (include "msg-queue.etcdFullname" .) -}}
{{- end -}}

{{/* cluster domain, overridable */}}
{{- define "msg-queue.clusterDomain" -}}
{{- default "cluster.local" .Values.clusterDomain -}}
{{- end -}}
