import type { AxiosResponse } from 'axios'
import { ref, type Ref } from 'vue'

import type { ApiResponse } from '@/api/client'
import { translate } from '@/plugins/i18n'
import { getErrorMessage } from '@/utils/error'
import { useUiStore, type SnackbarColor } from './ui'

type StoreRequestMode = 'loading' | 'saving' | 'none'
type CrudMessageName =
  | 'loadFailed'
  | 'createFailed'
  | 'updateFailed'
  | 'deleteFailed'
  | 'created'
  | 'updated'
  | 'deleted'
type ItemId = string | number
type ApiCall<T> = Promise<AxiosResponse<ApiResponse<T>>>

const NETWORK_ERROR_KEY = 'common.actions.message.networkError'
const OPERATION_FAILED_KEY = 'common.actions.message.operationFailed'

export type CrudI18nKey = string | Partial<Record<CrudMessageName, string>>

export interface CrudApi<T, CreatePayload, UpdatePayload, Id extends ItemId = string> {
  list: () => ApiCall<T[]>
  create?: (payload: CreatePayload) => ApiCall<T>
  update?: (id: Id, payload: UpdatePayload) => ApiCall<T>
  delete?: (id: Id) => ApiCall<unknown>
}

export interface CrudRequestOptions<ResponseData> {
  mode?: StoreRequestMode
  errorKey?: string | null
  successKey?: string | null
  successColor?: SnackbarColor
  failureNotifyKey?: string | null
  failureColor?: SnackbarColor
  expectData?: boolean
  onSuccess?: (data: ResponseData | undefined) => void | Promise<void>
}

export interface CrudRequestResult<ResponseData> {
  ok: boolean
  data?: ResponseData
}

interface CrudStateRefs {
  isLoading?: Ref<boolean>
  isSaving?: Ref<boolean>
  error?: Ref<string>
}

interface CreateRequestStoreOptions {
  state?: CrudStateRefs
}

interface CrudSuccessHooks<T, CreatePayload, UpdatePayload, Id extends ItemId> {
  onCreateSuccess?: (
    data: T,
    context: { payload: CreatePayload }
  ) => void | Promise<void>
  onUpdateSuccess?: (
    data: T,
    context: { id: Id; payload: UpdatePayload }
  ) => void | Promise<void>
  onDeleteSuccess?: (context: { id: Id }) => void | Promise<void>
}

interface CreateCrudStoreOptions<T, CreatePayload, UpdatePayload, Id extends ItemId> {
  api: CrudApi<T, CreatePayload, UpdatePayload, Id>
  i18nKey: CrudI18nKey
  idOf: (item: T) => Id
  items?: Ref<T[]>
  state?: CrudStateRefs
  hooks?: CrudSuccessHooks<T, CreatePayload, UpdatePayload, Id>
  listStaleTimeMs?: number
}

export interface FetchItemsOptions {
  force?: boolean
}

export const createRequestStore = (options: CreateRequestStoreOptions = {}) => {
  const uiStore = useUiStore()
  const isLoading = (options.state?.isLoading ?? ref(false)) as Ref<boolean>
  const isSaving = (options.state?.isSaving ?? ref(false)) as Ref<boolean>
  const error = (options.state?.error ?? ref('')) as Ref<string>

  const setBusy = (mode: StoreRequestMode, value: boolean) => {
    if (mode === 'loading') {
      isLoading.value = value
      return
    }

    if (mode === 'saving') {
      isSaving.value = value
    }
  }

  const runRequest = async <ResponseData>(
    request: () => ApiCall<ResponseData>,
    requestOptions: CrudRequestOptions<ResponseData> = {}
  ): Promise<CrudRequestResult<ResponseData>> => {
    const {
      mode = 'none',
      errorKey = OPERATION_FAILED_KEY,
      successKey,
      successColor = 'success',
      failureNotifyKey,
      failureColor = 'error',
      expectData = true,
      onSuccess,
    } = requestOptions

    setBusy(mode, true)
    error.value = ''

    try {
      const response = await request()
      if (response.data.success && (!expectData || response.data.data !== undefined)) {
        await onSuccess?.(response.data.data)

        if (successKey) {
          uiStore.showSnackbar(translate(successKey), successColor)
        }

        return {
          ok: true,
          data: response.data.data,
        }
      }

      error.value = response.data.error?.message || translate(errorKey ?? OPERATION_FAILED_KEY)
      if (failureNotifyKey) {
        uiStore.showSnackbar(translate(failureNotifyKey), failureColor)
      }
      return { ok: false }
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(errorDetail, translate(NETWORK_ERROR_KEY))
      return { ok: false }
    } finally {
      setBusy(mode, false)
    }
  }

  return {
    isLoading,
    isSaving,
    error,
    runRequest,
  }
}

export const createCrudStore = <T, CreatePayload, UpdatePayload, Id extends ItemId = string>(
  options: CreateCrudStoreOptions<T, CreatePayload, UpdatePayload, Id>
) => {
  const items = (options.items ?? ref<T[]>([])) as Ref<T[]>
  const requestStore = createRequestStore({ state: options.state })
  const { isLoading, isSaving, error, runRequest } = requestStore
  const listStaleTimeMs = Math.max(options.listStaleTimeMs ?? 0, 0)
  let listRequest: Promise<boolean> | null = null
  let lastFetchedAt = 0

  const resolveCrudKey = (name: CrudMessageName) => {
    if (typeof options.i18nKey === 'string') {
      return `${options.i18nKey}.${name}`
    }
    return options.i18nKey[name] ?? null
  }

  const appendItem = (item: T) => {
    items.value = [...items.value, item]
    lastFetchedAt = Date.now()
  }

  const replaceItem = (item: T) => {
    const index = items.value.findIndex((existing) => options.idOf(existing) === options.idOf(item))
    if (index !== -1) {
      items.value[index] = item
      lastFetchedAt = Date.now()
    }
  }

  const removeItem = (id: Id) => {
    items.value = items.value.filter((item) => options.idOf(item) !== id)
    lastFetchedAt = Date.now()
  }

  const setItems = (value: T[]) => {
    items.value = value
    lastFetchedAt = Date.now()
  }

  const fetchItems = async (fetchOptions: FetchItemsOptions = {}) => {
    if (listRequest) {
      return listRequest
    }

    const shouldUseCachedItems =
      !fetchOptions.force
      && listStaleTimeMs > 0
      && lastFetchedAt > 0
      && Date.now() - lastFetchedAt < listStaleTimeMs

    if (shouldUseCachedItems) {
      return true
    }

    const request = runRequest(() => options.api.list(), {
      mode: 'loading',
      errorKey: resolveCrudKey('loadFailed'),
      onSuccess: (data) => {
        setItems(data ?? [])
      },
    })
      .then((result) => result.ok)
      .finally(() => {
        listRequest = null
      })

    listRequest = request
    return request
  }

  const createItem = async (payload: CreatePayload) => {
    if (!options.api.create) {
      return null
    }

    const result = await runRequest(() => options.api.create!(payload), {
      mode: 'saving',
      errorKey: resolveCrudKey('createFailed'),
      successKey: resolveCrudKey('created'),
      successColor: 'success',
      onSuccess: async (data) => {
        if (data) {
          appendItem(data)
          await options.hooks?.onCreateSuccess?.(data, { payload })
        }
      },
    })

    return result.ok ? (result.data ?? null) : null
  }

  const updateItem = async (id: Id, payload: UpdatePayload) => {
    if (!options.api.update) {
      return null
    }

    const result = await runRequest(() => options.api.update!(id, payload), {
      mode: 'saving',
      errorKey: resolveCrudKey('updateFailed'),
      successKey: resolveCrudKey('updated'),
      successColor: 'success',
      onSuccess: async (data) => {
        if (data) {
          replaceItem(data)
          await options.hooks?.onUpdateSuccess?.(data, { id, payload })
        }
      },
    })

    return result.ok ? (result.data ?? null) : null
  }

  const deleteItem = async (id: Id) => {
    if (!options.api.delete) {
      return false
    }

    const result = await runRequest(() => options.api.delete!(id), {
      mode: 'saving',
      errorKey: resolveCrudKey('deleteFailed'),
      successKey: resolveCrudKey('deleted'),
      successColor: 'info',
      expectData: false,
      onSuccess: async () => {
        removeItem(id)
        await options.hooks?.onDeleteSuccess?.({ id })
      },
    })

    return result.ok
  }

  return {
    items,
    isLoading,
    isSaving,
    error,
    appendItem,
    replaceItem,
    removeItem,
    setItems,
    runRequest,
    fetchItems,
    createItem,
    updateItem,
    deleteItem,
  }
}