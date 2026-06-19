import { useMemo, useState, useCallback } from "react";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import DownloadIcon from "@mui/icons-material/Download";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import TablePagination from "@mui/material/TablePagination";
import TableSortLabel from "@mui/material/TableSortLabel";
import Paper from "@mui/material/Paper";
import Alert from "@mui/material/Alert";
import Collapse from "@mui/material/Collapse";
import IconButton from "@mui/material/IconButton";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowRightIcon from "@mui/icons-material/KeyboardArrowRight";
import JsonViewer from "./JsonViewer";

interface StateTableProps {
  entries: Record<string, unknown>[];
  columns?: string[];
  partialRead?: boolean;
  partialReadCause?: string;
}

type Order = "asc" | "desc";

function getCellValue(row: Record<string, unknown>, col: string): string {
  const val = row[col];
  if (val === null || val === undefined) return "";
  if (typeof val === "object") return JSON.stringify(val);
  return String(val);
}

function getPreview(row: Record<string, unknown>): string {
  const json = JSON.stringify(row);
  return json.length > 200 ? json.substring(0, 200) + "..." : json;
}

export default function StateTable({
  entries,
  columns,
  partialRead,
  partialReadCause
}: StateTableProps) {
  const [expandedRows, setExpandedRows] = useState<Set<number>>(new Set());
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);
  const [orderBy, setOrderBy] = useState<string>("");
  const [order, setOrder] = useState<Order>("asc");

  const toggleRow = (id: number) => {
    setExpandedRows(prev => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  const handleSort = (col: string) => {
    if (orderBy === col) {
      setOrder(prev => (prev === "asc" ? "desc" : "asc"));
    } else {
      setOrderBy(col);
      setOrder("asc");
    }
  };

  const headerCols = useMemo(() => {
    if (columns && columns.length > 0) return columns;
    return ["key", "__preview"];
  }, [columns]);

  const sortedEntries = useMemo(() => {
    const indexed = entries.map((entry, idx) => ({ idx, entry }));
    if (!orderBy) return indexed;

    return [...indexed].sort((a, b) => {
      const aVal = getCellValue(a.entry, orderBy);
      const bVal = getCellValue(b.entry, orderBy);
      const cmp = aVal.localeCompare(bVal, undefined, { numeric: true });
      return order === "asc" ? cmp : -cmp;
    });
  }, [entries, orderBy, order]);

  const visibleRows = sortedEntries.slice(
    page * rowsPerPage,
    page * rowsPerPage + rowsPerPage
  );

  const handleDownload = useCallback(() => {
    const exportData = entries.map(e => {
      const detail = (e as Record<string, unknown>)._detail;
      return detail || e;
    });
    const json = JSON.stringify(exportData, null, 2);
    const blob = new Blob([json], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `state-export-${entries.length}-entries.json`;
    a.click();
    URL.revokeObjectURL(url);
  }, [entries]);

  return (
    <Box>
      {partialRead && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          Partial read detected:{" "}
          {partialReadCause || "some state descriptors were unreadable"}
        </Alert>
      )}
      <Paper sx={{ width: "100%", overflow: "hidden" }}>
        <TableContainer sx={{ maxHeight: "70vh" }}>
          <Table size="small" stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell padding="checkbox" />
                {headerCols.map(col => (
                  <TableCell key={col}>
                    {col === "__preview" ? (
                      "Preview"
                    ) : (
                      <TableSortLabel
                        active={orderBy === col}
                        direction={orderBy === col ? order : "asc"}
                        onClick={() => handleSort(col)}
                      >
                        {col}
                      </TableSortLabel>
                    )}
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {visibleRows.map(({ idx, entry }) => {
                const isExpanded = expandedRows.has(idx);
                return (
                  <>
                    <TableRow
                      key={idx}
                      hover
                      onClick={() => toggleRow(idx)}
                      sx={{ cursor: "pointer" }}
                    >
                      <TableCell padding="checkbox">
                        <IconButton
                          size="small"
                          aria-label={
                            isExpanded ? "Collapse row" : "Expand row"
                          }
                        >
                          {isExpanded ? (
                            <KeyboardArrowDownIcon fontSize="small" />
                          ) : (
                            <KeyboardArrowRightIcon fontSize="small" />
                          )}
                        </IconButton>
                      </TableCell>
                      {headerCols.map(col => (
                        <TableCell
                          key={col}
                          sx={{
                            fontFamily: '"JetBrains Mono", monospace',
                            fontSize: "0.8rem",
                            maxWidth: 400,
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap"
                          }}
                        >
                          {col === "__preview"
                            ? getPreview(entry)
                            : getCellValue(entry, col)}
                        </TableCell>
                      ))}
                    </TableRow>
                    <TableRow key={`${idx}-detail`}>
                      <TableCell
                        colSpan={headerCols.length + 1}
                        sx={{
                          p: 0,
                          borderBottom: isExpanded ? undefined : "none"
                        }}
                      >
                        <Collapse in={isExpanded} timeout="auto" unmountOnExit>
                          <Box sx={{ p: 2 }}>
                            <JsonViewer
                              data={
                                (entry as Record<string, unknown>)._detail ||
                                entry
                              }
                            />
                          </Box>
                        </Collapse>
                      </TableCell>
                    </TableRow>
                  </>
                );
              })}
            </TableBody>
          </Table>
        </TableContainer>
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between"
          }}
        >
          <Button
            size="small"
            startIcon={<DownloadIcon />}
            onClick={handleDownload}
            sx={{ ml: 2 }}
          >
            Download JSON
          </Button>
          <TablePagination
            component="div"
            count={entries.length}
            page={page}
            onPageChange={(_e, newPage) => setPage(newPage)}
            rowsPerPage={rowsPerPage}
            onRowsPerPageChange={e => {
              setRowsPerPage(parseInt(e.target.value, 10));
              setPage(0);
            }}
            rowsPerPageOptions={[10, 25, 50, 100]}
          />
        </Box>
      </Paper>
    </Box>
  );
}
