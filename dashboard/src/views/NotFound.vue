<template>
  <v-container class="fill-height" fluid>
    <v-row align="center" justify="center">
      <v-col cols="12" sm="8" md="6" class="text-center">
        <v-icon icon="mdi-help-circle-outline" size="120" color="grey-lighten-1" class="mb-6" />
        <h1 class="text-h2 font-weight-bold mb-2">404</h1>
        <h2 class="text-h5 text-medium-emphasis mb-6">Page Not Found</h2>
        <p class="text-body-1 text-medium-emphasis mb-8">
          The page you're looking for doesn't exist or has been moved.
        </p>
        <v-btn color="primary" to="/dashboard" size="large" rounded="lg">
          Back to Dashboard
        </v-btn>
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup lang="ts">
</script>
