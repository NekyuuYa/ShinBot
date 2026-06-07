#!/usr/bin/env node

import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, extname, join, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const dashboardRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const srcRoot = join(dashboardRoot, 'src')
const args = parseArgs(process.argv.slice(2))
const outputPath = args.output ? resolve(dashboardRoot, args.output) : null
const outputFormat = args.format ?? 'markdown'
const maxHigh = parseOptionalNonNegativeInteger(args['max-high'], 'max-high')

const allowedRawColorFiles = new Set([
  'src/theme/themes.ts',
  'src/styles/_variables.scss',
])

const tokenDefinitionFiles = new Set([
  'src/styles/_variables.scss',
  'src/styles/_mixins.scss',
  'src/styles/_data-viz.scss',
])

const files = await collectFiles(srcRoot)
const findings = []
const summary = {
  scannedFiles: files.length,
  bySeverity: new Map(),
  byCategory: new Map(),
}

for (const filePath of files) {
  const relativePath = normalizePath(relative(dashboardRoot, filePath))
  const content = await readFile(filePath, 'utf8')
  const lines = content.split(/\r?\n/)
  const extension = extname(filePath)

  if (extension === '.vue') {
    auditVueFile(relativePath, content, lines)
  }

  if (['.vue', '.scss', '.ts'].includes(extension)) {
    auditStyleTokens(relativePath, content)
  }
}

for (const finding of findings) {
  summary.bySeverity.set(finding.severity, (summary.bySeverity.get(finding.severity) ?? 0) + 1)
  summary.byCategory.set(finding.category, (summary.byCategory.get(finding.category) ?? 0) + 1)
}

findings.sort((left, right) => {
  const severityOrder = { high: 0, medium: 1, low: 2 }
  return (
    severityOrder[left.severity] - severityOrder[right.severity] ||
    left.file.localeCompare(right.file) ||
    left.line - right.line
  )
})

const report = outputFormat === 'json' ? JSON.stringify({ summary: formatSummary(), findings }, null, 2) : renderMarkdown()

if (outputPath) {
  await mkdir(dirname(outputPath), { recursive: true })
  await writeFile(outputPath, `${report}\n`)
} else {
  process.stdout.write(`${report}\n`)
}

if (maxHigh !== null) {
  const highCount = summary.bySeverity.get('high') ?? 0
  if (highCount > maxHigh) {
    process.stderr.write(
      `Design audit failed: high findings ${highCount} exceed allowed maximum ${maxHigh}.\n`
    )
    process.exitCode = 1
  }
}

function auditVueFile(relativePath, content, lines) {
  if (lines.length > 600) {
    addFinding('high', 'maintainability', relativePath, 1, `Vue file has ${lines.length} lines. Split page orchestration, panels, and dialogs into focused components.`)
  } else if (lines.length > 300) {
    addFinding('medium', 'maintainability', relativePath, 1, `Vue file has ${lines.length} lines. Review whether template, state, or business transforms should be extracted.`)
  }

  const styleBlocks = [...content.matchAll(/<style\b([^>]*)>/g)]
  for (const match of styleBlocks) {
    const attributes = match[1] ?? ''
    if (!attributes.includes('lang="scss"') && !attributes.includes("lang='scss'")) {
      addFinding('medium', 'style-system', relativePath, lineOf(content, match.index), 'Style block does not use lang="scss", so it cannot directly consume ShinBot style tokens and mixins.')
    }
  }

  const cardCount = countMatches(content, /<v-card\b/g)
  if (cardCount >= 12) {
    addFinding('medium', 'visual-density', relativePath, 1, `Template contains ${cardCount} v-card elements. Check for nested cards, over-fragmented panels, and card-heavy layout.`)
  }

  if (/font-size\s*:\s*clamp\([^;]*vw/i.test(content)) {
    addFinding('high', 'typography', relativePath, lineOf(content, content.search(/font-size\s*:\s*clamp\([^;]*vw/i)), 'Font size scales with viewport width. Use fixed token sizes and responsive layout instead.')
  }
}

function auditStyleTokens(relativePath, content) {
  const isRawColorAllowed = allowedRawColorFiles.has(relativePath)
  const isTokenDefinition = tokenDefinitionFiles.has(relativePath)

  if (!isRawColorAllowed) {
    for (const match of content.matchAll(/#[0-9a-fA-F]{3,8}\b/g)) {
      addFinding('high', 'color-token', relativePath, lineOf(content, match.index), `Raw color ${match[0]} should move to theme or design tokens.`)
    }

    for (const match of content.matchAll(/\b(?:rgb|rgba|hsl|hsla)\([^)]*\)/g)) {
      const value = match[0]
      if (!value.includes('var(--v-theme-') && !value.includes('var(--v-border-')) {
        addFinding('medium', 'color-token', relativePath, lineOf(content, match.index), `Color function "${value}" is not theme-backed. Prefer Vuetify theme variables or ShinBot tokens.`)
      }
    }
  }

  if (!isTokenDefinition) {
    for (const match of content.matchAll(/\bbox-shadow\s*:|filter\s*:\s*drop-shadow\(/g)) {
      addFinding('medium', 'effect-token', relativePath, lineOf(content, match.index), 'Local shadow/drop-shadow should use $shadow-* or a shared mixin unless it is data-visualization specific.')
    }

    for (const match of content.matchAll(/border-radius\s*:\s*([^;]+)/g)) {
      const value = match[1].trim()
      if (value.startsWith('$') || value.startsWith('var(') || value === 'inherit') {
        continue
      }
      if (value === '50%') {
        continue
      }
      addFinding('medium', 'radius-token', relativePath, lineOf(content, match.index), `Local border radius "${value}" should use $radius-* or a component prop.`)
    }

    for (const match of content.matchAll(/\b(?:linear-gradient|radial-gradient)\(/g)) {
      addFinding('low', 'surface-token', relativePath, lineOf(content, match.index), 'Local gradient should be justified or moved into a named surface token/mixin.')
    }
  }

  for (const match of content.matchAll(/rgba\(\s*var\(--v-theme-[^)]+\)\s*,\s*([0-9.]+)\s*\)/g)) {
    if (!isTokenDefinition) {
      addFinding('low', 'opacity-token', relativePath, lineOf(content, match.index), `Theme opacity ${match[1]} is locally chosen. Consider a semantic text/surface/border token if this pattern repeats.`)
    }
  }
}

async function collectFiles(root) {
  const entries = await readdir(root, { withFileTypes: true })
  const result = []
  for (const entry of entries) {
    const path = join(root, entry.name)
    if (entry.isDirectory()) {
      result.push(...await collectFiles(path))
    } else if (['.vue', '.scss', '.ts'].includes(extname(entry.name))) {
      result.push(path)
    }
  }
  return result
}

function addFinding(severity, category, file, line, message) {
  findings.push({ severity, category, file, line, message })
}

function countMatches(content, regex) {
  return [...content.matchAll(regex)].length
}

function lineOf(content, index) {
  if (index < 0) {
    return 1
  }
  return content.slice(0, index).split(/\r?\n/).length
}

function normalizePath(path) {
  return path.replaceAll('\\', '/')
}

function parseArgs(argv) {
  const result = {}
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]
    if (!arg.startsWith('--')) {
      continue
    }

    const key = arg.slice(2)
    const next = argv[index + 1]
    if (next && !next.startsWith('--')) {
      result[key] = next
      index += 1
    } else {
      result[key] = 'true'
    }
  }
  return result
}

function parseOptionalNonNegativeInteger(value, name) {
  if (value === undefined) {
    return null
  }
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed < 0) {
    process.stderr.write(`Invalid --${name} value: expected a non-negative integer.\n`)
    process.exit(2)
  }
  return parsed
}

function formatSummary() {
  return {
    scannedFiles: summary.scannedFiles,
    bySeverity: Object.fromEntries(summary.bySeverity),
    byCategory: Object.fromEntries(summary.byCategory),
  }
}

function renderMarkdown() {
  const formattedSummary = formatSummary()
  const sections = [
    '# Dashboard Design Audit',
    '',
    `Scanned files: ${formattedSummary.scannedFiles}`,
    '',
    '## Summary',
    '',
    renderCounts('Severity', formattedSummary.bySeverity),
    '',
    renderCounts('Category', formattedSummary.byCategory),
    '',
    '## Findings',
    '',
  ]

  if (findings.length === 0) {
    sections.push('No findings.')
    return sections.join('\n')
  }

  for (const finding of findings) {
    sections.push(
      `- **${finding.severity}** \`${finding.category}\` \`${finding.file}:${finding.line}\` - ${finding.message}`
    )
  }
  return sections.join('\n')
}

function renderCounts(title, counts) {
  const entries = Object.entries(counts)
  if (entries.length === 0) {
    return `${title}: none`
  }
  return `${title}: ${entries.map(([key, value]) => `${key}=${value}`).join(', ')}`
}
