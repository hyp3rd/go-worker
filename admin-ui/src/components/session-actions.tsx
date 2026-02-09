"use client";

import { useRouter } from "next/navigation";
import { useTransition } from "react";
import { recordAuditEvent } from "@/lib/audit";
import { ConfirmDialog, useConfirmDialog } from "@/components/confirm-dialog";

export function SessionActions() {
  const router = useRouter();
  const [pending, startTransition] = useTransition();
  const { confirm, dialogProps } = useConfirmDialog();

  const handleLogout = async () => {
    const ok = await confirm({
      title: "Sign out from the admin console?",
      message: "Your session will be cleared on this device.",
      confirmLabel: "Sign out",
    });
    if (!ok) {
      return;
    }

    startTransition(async () => {
      await fetch("/api/auth/logout", { method: "POST" });
      recordAuditEvent({
        action: "logout",
        at: Date.now(),
        detail: "Signed out",
      });
      router.replace("/login");
    });
  };

  return (
    <>
      <button
        onClick={handleLogout}
        disabled={pending}
        className="rounded-full border border-soft px-3 py-1 text-xs font-semibold disabled:cursor-not-allowed disabled:opacity-60"
      >
        Sign out
      </button>
      <ConfirmDialog {...dialogProps} />
    </>
  );
}
