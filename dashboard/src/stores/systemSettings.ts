import { computed, ref } from 'vue'
import { defineStore } from 'pinia'

export type PricingCurrency = 'CNY' | 'USD'
export type PricingTokenUnit = 'mtokens' | 'ktokens'

const TOKEN_UNIT_FACTORS: Record<PricingTokenUnit, number> = {
  mtokens: 1_000_000,
  ktokens: 1_000,
}

export const useSystemSettingsStore = defineStore(
  'systemSettings',
  () => {
    const pricingCurrency = ref<PricingCurrency>('CNY')
    const pricingTokenUnit = ref<PricingTokenUnit>('mtokens')

    const pricingTokenDivisor = computed(
      () => TOKEN_UNIT_FACTORS[pricingTokenUnit.value] ?? TOKEN_UNIT_FACTORS.mtokens
    )

    const convertStoredPriceToDisplay = (pricePerMillion: number | null) => {
      if (pricePerMillion === null) {
        return null
      }
      return (pricePerMillion * pricingTokenDivisor.value) / 1_000_000
    }

    const convertDisplayPriceToStored = (displayPrice: number | null) => {
      if (displayPrice === null) {
        return null
      }
      return (displayPrice * 1_000_000) / pricingTokenDivisor.value
    }

    return {
      pricingCurrency,
      pricingTokenUnit,
      pricingTokenDivisor,
      convertStoredPriceToDisplay,
      convertDisplayPriceToStored,
    }
  },
  {
    persist: true,
  }
)
