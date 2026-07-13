function csvValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  const text = String(value).replace(/\r?\n/g, " ");
  if (/[",;]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

export function downloadCsv(filename: string, rows: Record<string, unknown>[], columns: string[]) {
  const header = columns.join(";");
  const body = rows.map((row) => columns.map((column) => csvValue(row[column])).join(";"));
  const blob = new Blob([[header, ...body].join("\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}
