{{/* Common labels. */}}
{{- define "gpucp-db.labels" -}}
app.kubernetes.io/part-of: gpu-control-plane
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/* Secret holding the dev-only credentials and DATABASE_URL. */}}
{{- define "gpucp-db.secretName" -}}postgres-credentials{{- end -}}
