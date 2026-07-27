import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { Login } from "../src/screens/Login";
import type { ApiClient, ApiUser } from "../src/types";

function apiWithFailure(status: number, detail: string, retryAfterSeconds?: number): ApiClient {
  return {
    request: vi.fn().mockResolvedValue(null),
    get: vi.fn().mockResolvedValue(null),
    post: vi.fn().mockResolvedValue(null),
    patch: vi.fn().mockResolvedValue(null),
    getLastError: () => ({ status, detail, retryAfterSeconds }),
  };
}

function renderLogin(api: ApiClient) {
  const setToken = vi.fn<(token: string | null) => void>();
  const setUser = vi.fn<(user: ApiUser | null) => void>();
  const notify = vi.fn<(message: string) => void>();
  render(<Login api={api} setToken={setToken} setUser={setUser} notify={notify} />);
  return { setToken, setUser, notify };
}

async function submitCredentials() {
  const user = userEvent.setup();
  await user.type(screen.getByLabelText("Usuário"), "admin");
  await user.type(screen.getByLabelText("Senha"), "senha-incorreta");
  await user.click(screen.getByRole("button", { name: "Entrar" }));
}

describe("Login feedback", () => {
  it("shows invalid credentials inline instead of failing silently", async () => {
    renderLogin(apiWithFailure(401, "Usuário ou senha inválidos"));
    await submitCredentials();
    expect(screen.getByRole("alert")).toHaveTextContent("Usuário ou senha inválidos");
  });

  it("explains the temporary lockout after too many attempts", async () => {
    renderLogin(apiWithFailure(429, "Muitas tentativas de login.", 300));
    await submitCredentials();
    expect(screen.getByRole("alert")).toHaveTextContent("Aguarde 5 minutos");
  });
});