import React from "react";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ProcessModal } from "../src/components/ProcessModal";

const processo = {
  id: 10,
  numero_cnj: "0000001-00.2026.8.11.0001",
  classe_processual: "Desapropriação",
  data_distribuicao: "2026-07-01",
  score_total: 88,
  faixa_probabilidade: "janela_quente",
};

function ModalHarness({ follow = vi.fn() }: { follow?: (value: typeof processo) => void }) {
  const [open, setOpen] = React.useState(false);
  return (
    <>
      <button onClick={() => setOpen(true)}>Abrir processo</button>
      {open && (
        <ProcessModal
          processo={processo}
          close={() => setOpen(false)}
          follow={follow}
        />
      )}
    </>
  );
}

describe("ProcessModal accessibility and actions", () => {
  it("focuses the first control, closes with Escape and restores focus", async () => {
    const user = userEvent.setup();
    render(<ModalHarness />);
    const opener = screen.getByRole("button", { name: "Abrir processo" });

    await user.click(opener);
    const firstClose = screen.getAllByRole("button", { name: "Fechar" })[0];
    expect(firstClose).toHaveFocus();

    await user.keyboard("{Escape}");

    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(opener).toHaveFocus();
  });

  it("closes from the backdrop but not from clicks inside the dialog", async () => {
    const user = userEvent.setup();
    render(<ModalHarness />);
    await user.click(screen.getByRole("button", { name: "Abrir processo" }));

    const dialog = screen.getByRole("dialog");
    await user.click(dialog);
    expect(dialog).toBeInTheDocument();

    await user.click(dialog.parentElement as HTMLElement);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("wraps Tab and Shift+Tab between the last and first controls", async () => {
    const user = userEvent.setup();
    render(<ModalHarness />);
    await user.click(screen.getByRole("button", { name: "Abrir processo" }));

    const first = screen.getAllByRole("button", { name: "Fechar" })[0];
    const last = screen.getByRole("button", { name: "Acompanhar processo" });
    last.focus();
    await user.tab();
    expect(first).toHaveFocus();

    await user.tab({ shift: true });
    expect(last).toHaveFocus();
  });

  it("passes the displayed process to the follow action", async () => {
    const user = userEvent.setup();
    const follow = vi.fn();
    render(<ModalHarness follow={follow} />);
    await user.click(screen.getByRole("button", { name: "Abrir processo" }));

    await user.click(screen.getByRole("button", { name: "Acompanhar processo" }));

    expect(follow).toHaveBeenCalledTimes(1);
    expect(follow).toHaveBeenCalledWith(processo);
  });
});
