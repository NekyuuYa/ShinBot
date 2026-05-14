import type {
  ConfigFieldDefinition,
  ConfigFieldType,
  ConfigProviderDefinition,
  ConfigProviderKind,
  ConfigRecord,
  ConfigValidationIssue,
  ConfigValue,
  ConfigWorkspaceProvider,
} from '@/api/config'
import {
  getConfigPathValue,
  hasConfigPathValue,
  joinConfigPath,
  setConfigPathValue,
} from './paths'
import { isVisibleWhenSatisfied } from './visibility'

export type ConfigFormComponent =
  | 'text'
  | 'number'
  | 'switch'
  | 'select'
  | 'string-list'
  | 'integer-list'
  | 'json'
  | 'array-object'

export interface ConfigFormOption {
  title: string
  value: ConfigValue
}

export interface ConfigFormField {
  key: string
  path: string
  providerKind: ConfigProviderKind
  providerId: string
  label: string
  description: string
  component: ConfigFormComponent
  valueType: ConfigFieldType
  inputType: 'text' | 'number' | 'password'
  required: boolean
  secret: boolean
  advanced: boolean
  deprecated: boolean
  placeholder: string
  visibleWhen: string
  metadata: Record<string, ConfigValue>
  options: ConfigFormOption[]
  issues: ConfigValidationIssue[]
  defaultValue?: ConfigValue
  min?: number
  max?: number
}

export interface BuildProviderFormFieldsOptions {
  issues?: ConfigValidationIssue[]
  pathPrefix?: string
  includeDeprecated?: boolean
}

export interface ProviderConfigFormModel {
  provider: ConfigProviderDefinition | ConfigWorkspaceProvider
  fields: ConfigFormField[]
  values: ConfigRecord
  issuesByPath: Record<string, ConfigValidationIssue[]>
}

const fieldComponentByType: Record<ConfigFieldType, ConfigFormComponent> = {
  string: 'text',
  integer: 'number',
  float: 'number',
  boolean: 'switch',
  enum: 'select',
  string_list: 'string-list',
  integer_list: 'integer-list',
  object: 'json',
  array_object: 'array-object',
  path: 'text',
  duration: 'number',
}

const isRecord = (value: ConfigValue | undefined): value is ConfigRecord =>
  Boolean(value) && typeof value === 'object' && !Array.isArray(value)

const cloneValue = <T extends ConfigValue | ConfigRecord>(value: T): T =>
  JSON.parse(JSON.stringify(value)) as T

function metadataString(field: ConfigFieldDefinition, key: string): string {
  const value = field.metadata[key]
  return typeof value === 'string' ? value.trim() : ''
}

function prettyFieldLabel(path: string): string {
  const segment = path.split('.').pop() || path
  return segment
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase())
}

function choiceTitle(field: ConfigFieldDefinition, value: ConfigValue): string {
  const labels = field.metadata.choice_labels
  if (isRecord(labels)) {
    const label = labels[String(value)]
    if (typeof label === 'string' && label.trim()) {
      return label.trim()
    }
  }
  return String(value)
}

function normalizeIssuePath(issue: ConfigValidationIssue, pathPrefix?: string): string {
  const prefix = pathPrefix?.trim()
  if (!prefix) {
    return issue.path
  }
  if (issue.path === prefix) {
    return ''
  }
  const dottedPrefix = `${prefix}.`
  return issue.path.startsWith(dottedPrefix)
    ? issue.path.slice(dottedPrefix.length)
    : issue.path
}

export function groupConfigIssuesByPath(
  issues: ConfigValidationIssue[] = [],
  pathPrefix?: string
): Record<string, ConfigValidationIssue[]> {
  const result: Record<string, ConfigValidationIssue[]> = {}
  for (const issue of issues) {
    const path = normalizeIssuePath(issue, pathPrefix)
    result[path] = result[path] || []
    result[path].push(issue)
  }
  return result
}

export function providerDefaults(
  provider: ConfigProviderDefinition | ConfigWorkspaceProvider
): ConfigRecord {
  if ('defaults' in provider) {
    return cloneValue(provider.defaults)
  }

  let result: ConfigRecord = {}
  for (const field of provider.fields) {
    if ('default' in field) {
      result = setConfigPathValue(result, field.path, cloneValue(field.default as ConfigValue))
    }
  }
  return result
}

export function mergeConfigRecords(base: ConfigRecord, override: ConfigRecord): ConfigRecord {
  const result: ConfigRecord = cloneValue(base)

  for (const [key, value] of Object.entries(override)) {
    const existing = result[key]
    if (isRecord(existing) && isRecord(value)) {
      result[key] = mergeConfigRecords(existing, value)
      continue
    }
    result[key] = cloneValue(value)
  }

  return result
}

export function createProviderConfigDraft(
  provider: ConfigProviderDefinition | ConfigWorkspaceProvider,
  currentConfig: ConfigRecord = {}
): ConfigRecord {
  return mergeConfigRecords(providerDefaults(provider), currentConfig)
}

export function buildProviderFormFields(
  provider: ConfigProviderDefinition | ConfigWorkspaceProvider,
  options: BuildProviderFormFieldsOptions = {}
): ConfigFormField[] {
  const issuesByPath = groupConfigIssuesByPath(options.issues, options.pathPrefix)

  return provider.fields
    .filter((field) => options.includeDeprecated || !field.deprecated)
    .map((field) => ({
      key: joinConfigPath(provider.kind, provider.id, field.path),
      path: field.path,
      providerKind: provider.kind,
      providerId: provider.id,
      label: metadataString(field, 'label') || metadataString(field, 'title') || prettyFieldLabel(field.path),
      description: field.description,
      component: fieldComponentByType[field.type],
      valueType: field.type,
      inputType: field.secret ? 'password' : field.type === 'integer' || field.type === 'float' || field.type === 'duration' ? 'number' : 'text',
      required: field.required,
      secret: field.secret,
      advanced: field.advanced,
      deprecated: field.deprecated,
      placeholder: field.placeholder,
      visibleWhen: field.visible_when,
      metadata: field.metadata,
      options: field.choices.map((value) => ({
        title: choiceTitle(field, value),
        value,
      })),
      issues: issuesByPath[field.path] ?? [],
      defaultValue: 'default' in field ? field.default : undefined,
      min: field.min,
      max: field.max,
    }))
}

export function createProviderConfigFormModel(
  provider: ConfigProviderDefinition | ConfigWorkspaceProvider,
  currentConfig: ConfigRecord = {},
  options: BuildProviderFormFieldsOptions = {}
): ProviderConfigFormModel {
  const values = createProviderConfigDraft(provider, currentConfig)
  return {
    provider,
    values,
    fields: buildProviderFormFields(provider, options),
    issuesByPath: groupConfigIssuesByPath(options.issues, options.pathPrefix),
  }
}

export function getProviderFieldValue(
  values: ConfigRecord,
  field: Pick<ConfigFormField, 'path' | 'defaultValue'>
): ConfigValue | undefined {
  const value = getConfigPathValue(values, field.path)
  return value === undefined ? field.defaultValue : value
}

export function setProviderFieldValue(
  values: ConfigRecord,
  field: Pick<ConfigFormField, 'path'>,
  value: ConfigValue
): ConfigRecord {
  return setConfigPathValue(values, field.path, value)
}

export function isProviderFieldVisible(field: ConfigFormField, values: ConfigRecord): boolean {
  return isVisibleWhenSatisfied(field.visibleWhen, values)
}

export function visibleProviderFields(
  fields: ConfigFormField[],
  values: ConfigRecord
): ConfigFormField[] {
  return fields.filter((field) => isProviderFieldVisible(field, values))
}

export function missingRequiredFields(
  fields: ConfigFormField[],
  values: ConfigRecord
): ConfigFormField[] {
  return visibleProviderFields(fields, values).filter((field) => {
    if (!field.required || hasConfigPathValue(values, field.path)) {
      return false
    }
    const value = getProviderFieldValue(values, field)
    return value === undefined || value === null || value === ''
  })
}
