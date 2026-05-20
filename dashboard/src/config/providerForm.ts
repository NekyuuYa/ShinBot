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
  | 'model-ref'

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
  locale?: string
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

function fieldComponentFor(field: ConfigFieldDefinition): ConfigFormComponent {
  const explicitComponent = metadataString(field, 'component')
  if (
    explicitComponent === 'model-ref' ||
    explicitComponent === 'text' ||
    explicitComponent === 'number' ||
    explicitComponent === 'switch' ||
    explicitComponent === 'select' ||
    explicitComponent === 'string-list' ||
    explicitComponent === 'integer-list' ||
    explicitComponent === 'json' ||
    explicitComponent === 'array-object'
  ) {
    return explicitComponent
  }
  return fieldComponentByType[field.type]
}

const isRecord = (value: ConfigValue | undefined): value is ConfigRecord =>
  Boolean(value) && typeof value === 'object' && !Array.isArray(value)

const cloneValue = <T extends ConfigValue | ConfigRecord>(value: T): T =>
  JSON.parse(JSON.stringify(value)) as T

function metadataString(field: ConfigFieldDefinition, key: string): string {
  const value = field.metadata[key]
  return typeof value === 'string' ? value.trim() : ''
}

function localizedKeys(locale = ''): string[] {
  const normalized = locale.trim().replace('_', '-')
  const fallback = normalized.toLowerCase().startsWith('zh') ? 'zh-CN' : 'en-US'
  const language = normalized.split('-')[0]
  return Array.from(new Set([
    normalized,
    normalized.toLowerCase(),
    language,
    fallback,
    fallback.toLowerCase(),
    fallback.split('-')[0],
  ].filter(Boolean)))
}

function localizedMetadata(metadata: Record<string, ConfigValue>, locale = ''): ConfigRecord | null {
  const i18n = metadata.i18n
  if (!isRecord(i18n)) {
    return null
  }

  for (const key of localizedKeys(locale)) {
    const value = i18n[key]
    if (isRecord(value)) {
      return value
    }
  }
  return null
}

function localizedMetadataString(
  metadata: Record<string, ConfigValue>,
  key: string,
  locale = ''
): string {
  const localized = localizedMetadata(metadata, locale)
  const value = localized?.[key]
  return typeof value === 'string' ? value.trim() : ''
}

function isChineseLocale(locale = ''): boolean {
  return locale.trim().toLowerCase().replace('_', '-').startsWith('zh')
}

function localizedProviderText(
  provider: ConfigProviderDefinition | ConfigWorkspaceProvider,
  key: 'display_name' | 'description',
  locale = ''
): string {
  return localizedMetadataString(provider.metadata, key, locale)
}

export function providerDisplayName(
  provider: ConfigProviderDefinition | ConfigWorkspaceProvider,
  locale = ''
): string {
  return localizedProviderText(provider, 'display_name', locale)
    || provider.display_name
    || provider.id
}

export function providerDescription(
  provider: ConfigProviderDefinition | ConfigWorkspaceProvider,
  locale = ''
): string {
  return localizedProviderText(provider, 'description', locale) || provider.description
}

function prettyFieldLabel(path: string): string {
  const segment = path.split('.').pop() || path
  return segment
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase())
}

function choiceTitle(field: ConfigFieldDefinition, value: ConfigValue, locale = ''): string {
  const localizedChoices = localizedMetadata(field.metadata, locale)?.choices
  if (isRecord(localizedChoices)) {
    const label = localizedChoices[String(value)]
    if (typeof label === 'string' && label.trim()) {
      return label.trim()
    }
  }

  const labels = field.metadata.choice_labels
  if (isRecord(labels)) {
    const label = labels[String(value)]
    if (typeof label === 'string' && label.trim()) {
      return label.trim()
    }
  }
  return String(value)
}

export function localizedConfigIssueMessage(
  issue: ConfigValidationIssue,
  locale = '',
  field?: ConfigFieldDefinition
): string {
  if (!isChineseLocale(locale)) {
    return issue.message
  }

  if (issue.code === 'required') {
    return '此字段为必填项'
  }
  if (issue.code === 'type') {
    return field ? `类型不正确，应为 ${field.type}` : '类型不正确'
  }
  if (issue.code === 'choices') {
    const choices = field?.choices.map((value) => choiceTitle(field, value, locale)).join('、')
    return choices ? `必须是以下选项之一：${choices}` : '不在允许的选项中'
  }
  if (issue.code === 'min') {
    return field?.min !== undefined ? `必须大于或等于 ${field.min}` : '小于允许的最小值'
  }
  if (issue.code === 'max') {
    return field?.max !== undefined ? `必须小于或等于 ${field.max}` : '超过允许的最大值'
  }
  if (issue.code === 'unknown') {
    return '未知配置项'
  }
  if (issue.code === 'unknown_ref') {
    return '引用的配置不存在'
  }
  if (issue.code === 'database_url') {
    return '当前只支持 SQLite 数据库地址'
  }
  if (issue.code === 'not_found') {
    return '文件不存在'
  }
  return issue.message
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
  const locale = options.locale ?? ''

  return provider.fields
    .filter((field) => options.includeDeprecated || !field.deprecated)
    .map((field) => ({
      key: joinConfigPath(provider.kind, provider.id, field.path),
      path: field.path,
      providerKind: provider.kind,
      providerId: provider.id,
      label: localizedMetadataString(field.metadata, 'label', locale)
        || metadataString(field, 'label')
        || metadataString(field, 'title')
        || prettyFieldLabel(field.path),
      description: localizedMetadataString(field.metadata, 'description', locale)
        || field.description,
      component: fieldComponentFor(field),
      valueType: field.type,
      inputType: field.secret ? 'password' : field.type === 'integer' || field.type === 'float' || field.type === 'duration' ? 'number' : 'text',
      required: field.required,
      secret: field.secret,
      advanced: field.advanced,
      deprecated: field.deprecated,
      placeholder: localizedMetadataString(field.metadata, 'placeholder', locale)
        || field.placeholder,
      visibleWhen: field.visible_when,
      metadata: field.metadata,
      options: field.choices.map((value) => ({
        title: choiceTitle(field, value, locale),
        value,
      })),
      issues: (issuesByPath[field.path] ?? []).map((issue) => ({
        ...issue,
        message: localizedConfigIssueMessage(issue, locale, field),
      })),
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
