{{- define "telemetry-pipeline.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{- define "telemetry-pipeline.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "telemetry-pipeline.labels" -}}
app.kubernetes.io/name: {{ include "telemetry-pipeline.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: Helm
app.kubernetes.io/version: "{{ .Chart.AppVersion }}"
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}
