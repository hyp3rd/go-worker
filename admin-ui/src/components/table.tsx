type TableColumn = {
  key: string;
  label: React.ReactNode;
};

export function Table({
  columns,
  children,
}: {
  columns: TableColumn[];
  children: React.ReactNode;
}) {
  return (
    <div className="overflow-hidden rounded-2xl border border-soft">
      <table className="w-full text-left text-sm">
        <thead className="bg-[var(--card)] text-xs uppercase tracking-[0.2em] text-muted">
          <tr>
            {columns.map((column) => (
              <th key={column.key} className="px-4 py-3">
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>{children}</tbody>
      </table>
    </div>
  );
}

export function TableRow({ children }: { children: React.ReactNode }) {
  return <tr className="border-t border-soft">{children}</tr>;
}

export function TableCell({
  children,
  align,
}: {
  children: React.ReactNode;
  align?: "left" | "right";
}) {
  return (
    <td
      className={`px-4 py-3 ${align === "right" ? "text-right" : "text-left"}`}
    >
      {children}
    </td>
  );
}
