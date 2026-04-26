export function providerSourceIcon(type: string) {
  if (type === "azure_openai") {
    return "mdi-microsoft-azure";
  }
  if (type === "ollama") {
    return "mdi-lan";
  }
  if (type === "custom_openai") {
    return "mdi-api";
  }
  if (type === "anthropic") {
    return "mdi-alpha-a-circle-outline";
  }
  if (type === "gemini") {
    return "mdi-google";
  }
  return "mdi-cloud-outline";
}
