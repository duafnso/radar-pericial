import { act, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useApiClient } from "../src/api/client";

const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
  vi.restoreAllMocks();
});

describe("ApiClient errors", () => {
  it("retains HTTP detail and Retry-After for login feedback", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue(new Response(
      JSON.stringify({ detail: "Muitas tentativas de login." }),
      {
        status: 429,
        headers: { "Content-Type": "application/json", "Retry-After": "300" },
      },
    ));
    const setToken = vi.fn();
    const setUser = vi.fn();
    const { result } = renderHook(() => useApiClient(null, setToken, setUser));

    await act(async () => {
      expect(await result.current.post("/api/login", {})).toBeNull();
    });

    expect(result.current.getLastError?.()).toEqual({
      status: 429,
      detail: "Muitas tentativas de login.",
      retryAfterSeconds: 300,
    });
  });
});
