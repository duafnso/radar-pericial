import type { Role, Screen } from "../types";

export const ROLE_PERMISSIONS: Record<Role, string[]> = {
  admin: ["read_data", "read_operational", "calculate_score", "create_perito", "run_collections", "manage_users", "view_audit"],
  operator: ["read_data", "read_operational", "calculate_score", "create_perito", "run_collections"],
  user: ["read_data", "calculate_score"],
  viewer: ["read_data"]
};

export const SCREEN_PERMISSIONS: Partial<Record<Screen, string>> = {
  coletas: "read_operational",
  usuarios: "manage_users",
  auditoria: "view_audit"
};
