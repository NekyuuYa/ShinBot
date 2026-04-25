import { ref } from 'vue'

interface CrudDialogOptions<T, P> {
  resetForm: () => void
  populateForm: (item: T) => void
  save: (payload: P, id?: string) => Promise<boolean>
  buildPayload: () => P
}

export function useCrudDialog<T extends { uuid?: string; id?: string }, P>(
  options: CrudDialogOptions<T, P>
) {
  const visible = ref(false)
  const editingId = ref('')
  const localError = ref('')
  const isSaving = ref(false)

  const openCreate = () => {
    editingId.value = ''
    localError.value = ''
    options.resetForm()
    visible.value = true
  }

  const openEdit = (item: T) => {
    editingId.value = item.uuid || item.id || ''
    localError.value = ''
    options.populateForm(item)
    visible.value = true
  }

  const submit = async () => {
    localError.value = ''
    isSaving.value = true
    try {
      const payload = options.buildPayload()
      const success = await options.save(payload, editingId.value)
      if (success) {
        visible.value = false
      }
    } catch (err: any) {
      localError.value = err.message || String(err)
    } finally {
      isSaving.value = false
    }
  }

  return {
    visible,
    editingId,
    localError,
    isSaving,
    openCreate,
    openEdit,
    submit,
  }
}
