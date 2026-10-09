import type {
  FedimintTotals,
  FederationSummary,
  GatewayInfo,
  GatewayUptimeTrendPoint,
  GatewayWindow,
} from '../types/api';

const BASE_URL = import.meta.env.VITE_FMO_API_BASE_URL || 'https://observer.fedimint.org/api';

export const api = {
  async getTotals(): Promise<FedimintTotals> {
    const response = await fetch(`${BASE_URL}/federations/totals`);
    if (!response.ok) {
      throw new Error('Failed to fetch totals');
    }
    return response.json();
  },

  async getFederations(): Promise<FederationSummary[]> {
    const response = await fetch(`${BASE_URL}/federations`);
    if (!response.ok) {
      throw new Error('Failed to fetch federations');
    }
    return response.json();
  },

  async getNostrFederations(): Promise<Record<string, string>> {
    const response = await fetch(`${BASE_URL}/nostr/federations`);
    if (!response.ok) {
      throw new Error('Failed to fetch nostr federations');
    }
    return response.json();
  },

  async getFederationGateways(
    id: string,
    window: GatewayWindow,
    signal?: AbortSignal,
  ): Promise<CachedResponse<GatewayInfo[]>> {
    return fetchCached(
      `${BASE_URL}/federations/${id}/gateways?window=${encodeURIComponent(window)}`,
      'Failed to fetch gateways',
      signal,
    );
  },

  async getFederationGatewayUptimeTrend(
    id: string,
    window: GatewayWindow,
    signal?: AbortSignal,
  ): Promise<CachedResponse<GatewayUptimeTrendPoint[]>> {
    return fetchCached(
      `${BASE_URL}/federations/${id}/gateways/uptime-trend?window=${encodeURIComponent(window)}`,
      'Failed to fetch the gateway uptime trend',
      signal,
    );
  },

  async getFederationGatewaysByInvite(inviteCode: string, signal?: AbortSignal): Promise<GatewayInfo[]> {
    const encodedInvite = encodeURIComponent(inviteCode);
    const response = await fetch(`${BASE_URL}/config/${encodedInvite}/gateways`, { signal });
    if (!response.ok) {
      throw new Error(`Failed to fetch gateways by invite (${response.status})`);
    }
    return response.json();
  },
};

export interface CachedResponse<T> {
  data: T;
  // Nginx answered from an expired cache entry and is refreshing it in the background
  stale: boolean;
}

async function fetchCached<T>(url: string, errorMessage: string, signal?: AbortSignal): Promise<CachedResponse<T>> {
  const response = await fetch(url, { signal });
  if (!response.ok) {
    throw new Error(`${errorMessage} (${response.status})`);
  }
  // Only readable on the same origin, which is how the site is served
  const cacheStatus = response.headers.get('X-Cache-Status');
  return { data: await response.json(), stale: cacheStatus === 'STALE' || cacheStatus === 'UPDATING' };
}
