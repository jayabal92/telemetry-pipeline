{{- define "telemetry-pipeline.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "telemetry-pipeline.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride -}}
{{- else -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "telemetry-pipeline.labels" -}}
app.kubernetes.io/name: {{ include "telemetry-pipeline.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "telemetry-pipeline.dbHost" -}}
{{- if .Values.db.host -}}
{{ .Values.db.host }}
{{- else -}}
{{ include "telemetry-pipeline.fullname" . }}-postgresql.{{ .Release.Namespace }}.svc.cluster.local
{{- end -}}
{{- end -}}
