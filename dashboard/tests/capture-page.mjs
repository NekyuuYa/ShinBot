#!/usr/bin/env node

import { mkdir, mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { spawn } from 'node:child_process'
import net from 'node:net'

const args = parseArgs(process.argv.slice(2))

const baseUrl = args.url ?? process.env.DASHBOARD_URL ?? 'http://localhost:3000'
const route = args.route ?? '/monitoring'
const outputPath = resolve(args.output ?? 'monitoring.png')
const width = Number(args.width ?? 1440)
const height = Number(args.height ?? 1400)
const loginPath = args.loginPath ?? '/login'
const username = args.username ?? process.env.DASHBOARD_USERNAME ?? 'admin'
const password = args.password ?? process.env.DASHBOARD_PASSWORD ?? 'admin'
const explicitToken = args.token ?? process.env.DASHBOARD_TOKEN ?? ''
const chromiumBinary = args.chromium ?? process.env.CHROMIUM_BIN ?? '/usr/bin/chromium'

await mkdir(resolve(outputPath, '..'), { recursive: true })

const token = explicitToken || (await fetchToken(baseUrl, username, password))
const port = await getFreePort()
const userDataDir = await mkdtemp(join(tmpdir(), 'shinbot-chromium-'))

const chromiumArgs = [
  '--headless=new',
  '--no-sandbox',
  '--disable-gpu',
  '--disable-dev-shm-usage',
  '--hide-scrollbars',
  '--force-device-scale-factor=1',
  `--window-size=${width},${height}`,
  `--remote-debugging-port=${port}`,
  `--user-data-dir=${userDataDir}`,
  'about:blank',
]

const chromium = spawn(chromiumBinary, chromiumArgs, {
  stdio: ['ignore', 'ignore', 'pipe'],
})

chromium.stderr.on('data', (chunk) => {
  const text = String(chunk)
  if (!text.includes('shared_memory_switch')) {
    process.stderr.write(text)
  }
})

try {
  const wsUrl = await waitForPageTarget(port)
  const cdp = await connectCdp(wsUrl)

  try {
    await cdp.send('Page.enable')
    await cdp.send('Runtime.enable')

    await navigate(cdp, `${baseUrl}${loginPath}`)
    await delay(500)

    if (token) {
      await cdp.send('Runtime.evaluate', {
        expression: `localStorage.setItem('auth_token', ${JSON.stringify(token)});`,
        awaitPromise: false,
        returnByValue: true,
      })
    }

    await navigate(cdp, `${baseUrl}${route}`)
    await waitForPageReady(cdp)

    await cdp.send('Runtime.evaluate', {
      expression: `(() => {
        history.scrollRestoration = 'manual';
        window.scrollTo({ top: 0, left: 0, behavior: 'instant' });
        const main = document.querySelector('.main-content-area');
        const scroller = document.querySelector('.main-content-area .v-main__scroller');
        if (main) {
          main.scrollTop = 0;
          main.scrollLeft = 0;
        }
        if (scroller) {
          scroller.scrollTop = 0;
          scroller.scrollLeft = 0;
        }
      })()`,
      awaitPromise: false,
      returnByValue: true,
    })
    await delay(150)

    const png = await cdp.send('Page.captureScreenshot', {
      format: 'png',
      fromSurface: true,
    })
    await writeFile(outputPath, Buffer.from(png.data, 'base64'))
    process.stdout.write(`Screenshot saved to ${outputPath}\n`)
  } finally {
    cdp.close()
  }
} finally {
  chromium.kill('SIGTERM')
  await delay(250)
  await rm(userDataDir, { recursive: true, force: true })
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

async function fetchToken(baseUrl, username, password) {
  const response = await fetch(`${baseUrl}/api/v1/auth/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ username, password }),
  })

  if (!response.ok) {
    throw new Error(`Login request failed with HTTP ${response.status}`)
  }

  const payload = await response.json()
  const token = payload?.data?.token
  if (!token) {
    throw new Error('Login response did not include a token')
  }
  return token
}

async function getFreePort() {
  return await new Promise((resolvePort, rejectPort) => {
    const server = net.createServer()
    server.listen(0, '127.0.0.1', () => {
      const address = server.address()
      if (address && typeof address === 'object') {
        const { port } = address
        server.close(() => resolvePort(port))
      } else {
        server.close(() => rejectPort(new Error('Unable to allocate a free port')))
      }
    })
    server.on('error', rejectPort)
  })
}

async function waitForPageTarget(port) {
  const targetsUrl = `http://127.0.0.1:${port}/json/list`
  for (let attempt = 0; attempt < 60; attempt += 1) {
    try {
      const response = await fetch(targetsUrl)
      if (response.ok) {
        const payload = await response.json()
        const pageTarget = Array.isArray(payload)
          ? payload.find((target) => target.type === 'page' && target.webSocketDebuggerUrl)
          : null
        if (pageTarget?.webSocketDebuggerUrl) {
          return pageTarget.webSocketDebuggerUrl
        }
      }
    } catch {
      // keep retrying
    }
    await delay(250)
  }
  throw new Error('Chromium page target did not become ready')
}

async function connectCdp(wsUrl) {
  const socket = new WebSocket(wsUrl)
  await new Promise((resolveSocket, rejectSocket) => {
    socket.addEventListener('open', () => resolveSocket())
    socket.addEventListener('error', () => rejectSocket(new Error('Failed to open DevTools WebSocket')))
  })

  let nextId = 1
  const pending = new Map()
  const eventListeners = new Map()

  socket.addEventListener('message', (event) => {
    const message = JSON.parse(event.data)
    if (message.id) {
      const record = pending.get(message.id)
      if (!record) {
        return
      }
      pending.delete(message.id)
      if (message.error) {
        record.reject(new Error(message.error.message || 'CDP command failed'))
      } else {
        record.resolve(message.result ?? {})
      }
      return
    }

    const listeners = eventListeners.get(message.method)
    if (listeners) {
      for (const listener of listeners) {
        listener(message.params ?? {})
      }
    }
  })

  return {
    async send(method, params = {}) {
      const id = nextId++
      const payload = { id, method, params }
      const promise = new Promise((resolveCommand, rejectCommand) => {
        pending.set(id, { resolve: resolveCommand, reject: rejectCommand })
      })
      socket.send(JSON.stringify(payload))
      return await promise
    },
    on(method, listener) {
      const listeners = eventListeners.get(method) ?? []
      listeners.push(listener)
      eventListeners.set(method, listeners)
    },
    close() {
      socket.close()
    },
  }
}

async function navigate(cdp, url) {
  await cdp.send('Page.navigate', { url })
  await delay(900)
}

async function waitForPageReady(cdp) {
  await cdp.send('Runtime.evaluate', {
    expression: `new Promise((resolve) => {
      const done = () => resolve(document.readyState);
      if (document.readyState === 'complete') {
        done();
        return;
      }
      window.addEventListener('load', done, { once: true });
    })`,
    awaitPromise: true,
    returnByValue: true,
  })
  await delay(1200)
}

function delay(ms) {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms))
}