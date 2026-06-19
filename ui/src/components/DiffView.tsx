import { useState, useCallback } from "react";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import DownloadIcon from "@mui/icons-material/Download";
import Card from "@mui/material/Card";
import CardActionArea from "@mui/material/CardActionArea";
import CardContent from "@mui/material/CardContent";
import Chip from "@mui/material/Chip";
import Collapse from "@mui/material/Collapse";
import Typography from "@mui/material/Typography";
import Alert from "@mui/material/Alert";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Paper from "@mui/material/Paper";
import IconButton from "@mui/material/IconButton";
import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import type { DiffResultData, DiffEntryData } from "../api/client";
import JsonViewer from "./JsonViewer";

interface DiffViewProps {
  diff: DiffResultData;
  partialRead?: boolean;
  partialReadCause?: string;
}

type Category = "summary" | "added" | "removed" | "modified";

export default function DiffView({
  diff,
  partialRead,
  partialReadCause
}: DiffViewProps) {
  const [activeCategory, setActiveCategory] = useState<Category>("summary");
  const [selectedEntry, setSelectedEntry] = useState<DiffEntryData | null>(
    null
  );

  const handleDownload = useCallback(() => {
    const json = JSON.stringify(diff, null, 2);
    const blob = new Blob([json], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `diff-export-${diff.operatorName}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }, [diff]);

  if (selectedEntry) {
    return (
      <Box>
        <Box sx={{ mb: 2, display: "flex", alignItems: "center", gap: 1 }}>
          <IconButton
            onClick={() => setSelectedEntry(null)}
            aria-label="Back to category list"
          >
            <ArrowBackIcon />
          </IconButton>
          <Typography
            variant="h6"
            sx={{ fontFamily: '"JetBrains Mono", monospace' }}
          >
            {selectedEntry.key}
          </Typography>
        </Box>
        <EntryDetail entry={selectedEntry} diff={diff} />
      </Box>
    );
  }

  if (activeCategory !== "summary") {
    const entries =
      activeCategory === "added"
        ? diff.added
        : activeCategory === "removed"
        ? diff.removed
        : diff.modified;

    return (
      <Box>
        <Box sx={{ mb: 2, display: "flex", alignItems: "center", gap: 1 }}>
          <IconButton
            onClick={() => setActiveCategory("summary")}
            aria-label="Back to summary"
          >
            <ArrowBackIcon />
          </IconButton>
          <Typography variant="h6" sx={{ textTransform: "capitalize" }}>
            {activeCategory} ({entries.length})
          </Typography>
        </Box>
        <EntryList
          entries={entries}
          category={activeCategory}
          onSelect={setSelectedEntry}
        />
      </Box>
    );
  }

  return (
    <Box>
      {partialRead && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          Partial read detected: {partialReadCause}
        </Alert>
      )}
      <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 2 }}>
        {diff.label1} &rarr; {diff.label2}
      </Typography>
      <Box sx={{ mb: 2 }}>
        <Button
          size="small"
          startIcon={<DownloadIcon />}
          onClick={handleDownload}
        >
          Download JSON
        </Button>
      </Box>
      <Box sx={{ display: "flex", gap: 2, flexWrap: "wrap", mb: 3 }}>
        <SummaryCard
          label="Added"
          count={diff.added.length}
          color="success.main"
          onClick={() => diff.added.length > 0 && setActiveCategory("added")}
          disabled={diff.added.length === 0}
        />
        <SummaryCard
          label="Removed"
          count={diff.removed.length}
          color="error.main"
          onClick={() =>
            diff.removed.length > 0 && setActiveCategory("removed")
          }
          disabled={diff.removed.length === 0}
        />
        <SummaryCard
          label="Modified"
          count={diff.modified.length}
          color="warning.main"
          onClick={() =>
            diff.modified.length > 0 && setActiveCategory("modified")
          }
          disabled={diff.modified.length === 0}
        />
        <SummaryCard
          label="Unchanged"
          count={diff.unchangedCount}
          color="text.secondary"
          disabled
        />
      </Box>
    </Box>
  );
}

function SummaryCard({
  label,
  count,
  color,
  onClick,
  disabled
}: {
  label: string;
  count: number;
  color: string;
  onClick?: () => void;
  disabled?: boolean;
}) {
  return (
    <Card sx={{ minWidth: 150, opacity: disabled ? 0.6 : 1 }}>
      <CardActionArea onClick={onClick} disabled={disabled}>
        <CardContent sx={{ textAlign: "center" }}>
          <Typography variant="h3" sx={{ color, fontWeight: 700 }}>
            {count}
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            {label}
          </Typography>
        </CardContent>
      </CardActionArea>
    </Card>
  );
}

function EntryList({
  entries,
  category,
  onSelect
}: {
  entries: DiffEntryData[];
  category: string;
  onSelect: (entry: DiffEntryData) => void;
}) {
  return (
    <Paper sx={{ overflow: "auto" }}>
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>Key</TableCell>
            {category === "modified" && <TableCell>Changes</TableCell>}
          </TableRow>
        </TableHead>
        <TableBody>
          {entries.map((entry, idx) => (
            <TableRow
              key={idx}
              hover
              onClick={() => onSelect(entry)}
              sx={{ cursor: "pointer" }}
            >
              <TableCell
                sx={{
                  fontFamily: '"JetBrains Mono", monospace',
                  fontSize: "0.8rem"
                }}
              >
                {entry.key}
              </TableCell>
              {category === "modified" && (
                <TableCell>
                  <Chip
                    size="small"
                    label={`${entry.fieldChanges?.length ?? 0} fields`}
                    color="warning"
                    variant="outlined"
                  />
                </TableCell>
              )}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </Paper>
  );
}

function EntryDetail({
  entry,
  diff
}: {
  entry: DiffEntryData;
  diff: DiffResultData;
}) {
  if (entry.json1 === null && entry.json2 !== null) {
    return (
      <Box>
        <Chip label="Added" color="success" size="small" sx={{ mb: 2 }} />
        <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1 }}>
          Present in {diff.label2} only
        </Typography>
        <JsonViewer data={entry.json2} />
      </Box>
    );
  }

  if (entry.json2 === null && entry.json1 !== null) {
    return (
      <Box>
        <Chip label="Removed" color="error" size="small" sx={{ mb: 2 }} />
        <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1 }}>
          Present in {diff.label1} only
        </Typography>
        <JsonViewer data={entry.json1} />
      </Box>
    );
  }

  return (
    <Box>
      <Chip label="Modified" color="warning" size="small" sx={{ mb: 2 }} />
      {entry.fieldChanges && entry.fieldChanges.length > 0 && (
        <Paper sx={{ mb: 3, overflow: "auto" }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Field</TableCell>
                <TableCell>{diff.label1}</TableCell>
                <TableCell>{diff.label2}</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {entry.fieldChanges.map((fc, idx) => (
                <TableRow key={idx}>
                  <TableCell
                    sx={{
                      fontFamily: '"JetBrains Mono", monospace',
                      fontWeight: 600,
                      verticalAlign: "top"
                    }}
                  >
                    {fc.fieldName}
                  </TableCell>
                  <TableCell
                    sx={{
                      bgcolor: "rgba(244, 67, 54, 0.1)",
                      maxWidth: 500,
                      verticalAlign: "top"
                    }}
                  >
                    <FieldValue value={fc.oldValue} />
                  </TableCell>
                  <TableCell
                    sx={{
                      bgcolor: "rgba(102, 187, 106, 0.1)",
                      maxWidth: 500,
                      verticalAlign: "top"
                    }}
                  >
                    <FieldValue value={fc.newValue} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Paper>
      )}
      <Box sx={{ display: "flex", gap: 2 }}>
        <Box sx={{ flex: 1 }}>
          <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1 }}>
            {diff.label1}
          </Typography>
          <JsonViewer data={entry.json1} />
        </Box>
        <Box sx={{ flex: 1 }}>
          <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1 }}>
            {diff.label2}
          </Typography>
          <JsonViewer data={entry.json2} />
        </Box>
      </Box>
    </Box>
  );
}

function FieldValue({ value }: { value: string | null }) {
  const [expanded, setExpanded] = useState(false);

  if (value === null || value === undefined) {
    return <em>absent</em>;
  }

  let formatted = value;
  try {
    const parsed = JSON.parse(value);
    formatted = JSON.stringify(parsed, null, 2);
  } catch {
    // Not JSON
  }

  const isLong = formatted.length > 100;

  if (!isLong) {
    return (
      <Box
        component="pre"
        sx={{
          m: 0,
          fontFamily: '"JetBrains Mono", monospace',
          fontSize: "0.8rem",
          whiteSpace: "pre-wrap",
          wordBreak: "break-all"
        }}
      >
        {formatted}
      </Box>
    );
  }

  return (
    <Box>
      <Collapse in={expanded} collapsedSize={40}>
        <Box
          component="pre"
          sx={{
            m: 0,
            fontFamily: '"JetBrains Mono", monospace',
            fontSize: "0.8rem",
            whiteSpace: "pre-wrap",
            wordBreak: "break-all"
          }}
        >
          {formatted}
        </Box>
      </Collapse>
      <Button
        size="small"
        onClick={() => setExpanded(prev => !prev)}
        sx={{ mt: 0.5, textTransform: "none", fontSize: "0.75rem" }}
      >
        {expanded ? "Hide" : "Show"} ({formatted.split("\n").length} lines)
      </Button>
    </Box>
  );
}
