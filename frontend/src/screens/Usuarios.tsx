import React from "react";
import { Download, Plus, RefreshCw, Save, X } from "lucide-react";
import { DataTable } from "../components/DataTable";
import { Page } from "../components/Page";
import type { ApiClient } from "../types";
import { downloadCsv } from "../utils/export";
import { shortDate } from "../utils/format";

const emptyForm = { username: "", password: "", role: "user", regiao_foco: "" };

export function Usuarios({ api, hasPermission, notify }: {
  api: ApiClient;
  hasPermission: (permission?: string) => boolean;
  notify: (message: string) => void;
}) {
  const [items, setItems] = React.useState<any[]>([]);
  const [showCreate, setShowCreate] = React.useState(false);
  const [form, setForm] = React.useState(emptyForm);
  const [filters, setFilters] = React.useState({ role: "", ativo: "", busca: "" });

  async function load() {
    if (!hasPermission("manage_users")) return;
    const params = new URLSearchParams();
    if (filters.role) params.set("role", filters.role);
    if (filters.ativo) params.set("ativo", filters.ativo);
    if (filters.busca) params.set("busca", filters.busca);
    const query = params.toString();
    const data = await api.get<any>(`/api/admin/usuarios${query ? `?${query}` : ""}`);
    setItems(data?.items || []);
  }

  React.useEffect(() => { load(); }, [filters]);

  function exportUsers() {
    downloadCsv("usuarios-radar-pericial.csv", items, [
      "id",
      "username",
      "role",
      "ativo",
      "regiao_foco",
      "criado_em"
    ]);
  }

  async function changeRole(id: number, role: string) {
    const result = await api.patch<any>(`/api/admin/usuarios/${id}/role`, { role });
    notify(result?.status === "ok" ? "Permissão atualizada." : "Não foi possível atualizar permissão.");
    load();
  }

  async function changeStatus(id: number, ativo: boolean) {
    const result = await api.patch<any>(`/api/admin/usuarios/${id}/ativo`, { ativo });
    notify(result?.status === "ok" ? "Status atualizado." : "Não foi possível atualizar status.");
    load();
  }

  async function createUser() {
    const payload = {
      ...form,
      username: form.username.trim().toLowerCase(),
      regiao_foco: form.regiao_foco || null
    };
    const result = await api.post<any>("/api/admin/usuarios", payload);
    if (result?.status === "created") {
      notify("Usuário criado com sucesso.");
      setForm(emptyForm);
      setShowCreate(false);
      load();
      return;
    }
    notify("Não foi possível criar usuário. Verifique login e senha.");
  }

  return (
    <Page
      title="Usuários e Permissões"
      subtitle={`${items.length} usuários cadastrados`}
      action={
        <div className="button-row">
          <button onClick={exportUsers} disabled={!items.length}><Download size={14} /> CSV</button>
          <button className="primary" onClick={() => setShowCreate(true)}><Plus size={14} /> Adicionar usuário</button>
          <button onClick={load}><RefreshCw size={14} /> Atualizar</button>
        </div>
      }
    >
      <div className="filters">
        <input placeholder="Buscar usuário" value={filters.busca} onChange={(event) => setFilters({ ...filters, busca: event.target.value })} />
        <select value={filters.role} onChange={(event) => setFilters({ ...filters, role: event.target.value })}>
          <option value="">Todos os perfis</option>
          <option value="admin">admin</option>
          <option value="operator">operator</option>
          <option value="user">user</option>
          <option value="viewer">viewer</option>
        </select>
        <select value={filters.ativo} onChange={(event) => setFilters({ ...filters, ativo: event.target.value })}>
          <option value="">Todos os status</option>
          <option value="true">Ativos</option>
          <option value="false">Inativos</option>
        </select>
      </div>

      {showCreate && (
        <section className="card">
          <div className="card-header-row">
            <div>
              <div className="section-label">Novo usuário</div>
              <p>Defina login, senha temporária e perfil de permissão.</p>
            </div>
            <button className="secondary icon-button" onClick={() => setShowCreate(false)} aria-label="Fechar"><X size={16} /></button>
          </div>
          <div className="form-grid">
            <label>
              Usuário
              <input value={form.username} onChange={(event) => setForm({ ...form, username: event.target.value })} placeholder="nome.sobrenome" />
            </label>
            <label>
              Senha temporária
              <input type="password" value={form.password} onChange={(event) => setForm({ ...form, password: event.target.value })} placeholder="mínimo 8 caracteres" />
            </label>
            <label>
              Permissão
              <select value={form.role} onChange={(event) => setForm({ ...form, role: event.target.value })}>
                <option value="admin">admin - acesso total</option>
                <option value="operator">operator - coletas e operação</option>
                <option value="user">user - uso do radar e calculadora</option>
                <option value="viewer">viewer - leitura</option>
              </select>
            </label>
            <label>
              Região de foco
              <select value={form.regiao_foco} onChange={(event) => setForm({ ...form, regiao_foco: event.target.value })}>
                <option value="">Sem restrição regional</option>
                <option>Médio-Norte</option>
                <option>Norte</option>
                <option>Centro-Sul</option>
                <option>Oeste</option>
                <option>Leste</option>
                <option>Sudoeste</option>
              </select>
            </label>
          </div>
          <div className="button-row">
            <button className="primary" onClick={createUser}><Save size={14} /> Salvar usuário</button>
            <button className="secondary" onClick={() => setShowCreate(false)}>Cancelar</button>
          </div>
        </section>
      )}

      <DataTable headers={["ID", "Usuário", "Permissão", "Status", "Região", "Criado em"]}>
        {items.map((item) => (
          <tr key={item.id}>
            <td>{item.id}</td>
            <td>{item.username}</td>
            <td>
              <select value={item.role} onChange={(event) => changeRole(item.id, event.target.value)}>
                {["admin", "operator", "user", "viewer"].map((role) => <option key={role}>{role}</option>)}
              </select>
            </td>
            <td>
              <select value={item.ativo ? "ativo" : "inativo"} onChange={(event) => changeStatus(item.id, event.target.value === "ativo")}>
                <option value="ativo">ativo</option>
                <option value="inativo">inativo</option>
              </select>
            </td>
            <td>{item.regiao_foco || "--"}</td>
            <td>{shortDate(item.criado_em)}</td>
          </tr>
        ))}
      </DataTable>
    </Page>
  );
}
