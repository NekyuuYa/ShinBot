import { onScopeDispose, ref, watch, type Ref } from 'vue'

export function useDelayedFlag(source: Readonly<Ref<boolean>>, delayMs = 180) {
  const delayed = ref(false)
  let timer: ReturnType<typeof setTimeout> | undefined

  const clearTimer = () => {
    if (timer) {
      clearTimeout(timer)
      timer = undefined
    }
  }

  watch(
    source,
    (value) => {
      clearTimer()

      if (!value) {
        delayed.value = false
        return
      }

      timer = setTimeout(() => {
        delayed.value = true
        timer = undefined
      }, delayMs)
    },
    { immediate: true }
  )

  onScopeDispose(clearTimer)

  return delayed
}
