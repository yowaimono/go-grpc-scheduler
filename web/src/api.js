const runtimeInstances = window.__SCHEDULER_CONFIG__?.instances?.join(',')
const configured = (runtimeInstances || import.meta.env.VITE_SCHEDULER_INSTANCES || 'http://localhost:8080')
  .split(',').map((item) => item.trim()).filter(Boolean)
const apiToken = window.__SCHEDULER_CONFIG__?.apiToken || import.meta.env.VITE_SCHEDULER_API_TOKEN || ''

let active = null

async function request(base, path, options = {}) {
  const response = await fetch(`${base.replace(/\/$/, '')}${path}`, {
    ...options,
    headers: { 'Content-Type': 'application/json', ...(apiToken ? { Authorization: `Bearer ${apiToken}` } : {}), ...(options.headers || {}) },
  })
  if (!response.ok) {
    const error = new Error(`HTTP ${response.status}`)
    error.status = response.status
    throw error
  }
  return response.json()
}

export async function discover() {
  const probes = await Promise.allSettled(configured.map((base) => request(base, '/api/v1/discovery')))
  const candidates = probes.flatMap((result, index) => result.status === 'fulfilled' ? [{ base: configured[index], ...result.value }] : [])
  const master = candidates.find((item) => item.role === 'master' && item.ready)
  if (!master) throw new Error('no master scheduler available')
  active = master.base
  return master
}

export async function api(path, options = {}) {
  if (!active) await discover()
  try {
    return await request(active, path, options)
  } catch (error) {
    if (![409, 502, 503, 504].includes(error.status)) throw error
    await discover()
    return request(active, path, options)
  }
}

export function instances() { return configured }
