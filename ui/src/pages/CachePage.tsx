import { useCallback, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import Paper from "@mui/material/Paper";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Button from "@mui/material/Button";
import Chip from "@mui/material/Chip";
import Alert from "@mui/material/Alert";
import CachedIcon from "@mui/icons-material/Cached";
import { listCache, deleteCache, type CacheEntry } from "../api/client";

function formatBytes(bytes: number): string {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / (1024 * 1024)).toFixed(1) + " MB";
}

function formatTime(epochMs: number): string {
  return new Date(epochMs).toLocaleString();
}

function sourceLabel(sourcePath: string): string {
  const match = sourcePath.match(/chk-\d+|savepoint-[a-z0-9]+/);
  return match ? match[0] : sourcePath.split("/").pop() || sourcePath;
}

export default function CachePage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [diffSelection, setDiffSelection] = useState<CacheEntry | null>(null);

  const { data: entries, isLoading, isError } = useQuery({
    queryKey: ["cache"],
    queryFn: listCache,
    refetchInterval: 10_000
  });

  const deleteMutation = useMutation({
    mutationFn: deleteCache,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["cache"] })
  });

  const handleDelete = useCallback(
    (id: string) => {
      deleteMutation.mutate(id);
      if (diffSelection?.id === id) setDiffSelection(null);
    },
    [deleteMutation, diffSelection]
  );

  const handleInspect = useCallback(
    (entry: CacheEntry) => {
      navigate(`/inspect?path=${encodeURIComponent(entry.sourcePath)}`);
    },
    [navigate]
  );

  const handleDiffClick = useCallback(
    (entry: CacheEntry) => {
      if (!diffSelection) {
        setDiffSelection(entry);
      } else {
        navigate(
          `/diff?path1=${encodeURIComponent(diffSelection.sourcePath)}&path2=${encodeURIComponent(entry.sourcePath)}`
        );
        setDiffSelection(null);
      }
    },
    [diffSelection, navigate]
  );

  const totalSize = entries?.reduce((sum, e) => sum + e.sizeBytes, 0) ?? 0;

  return (
    <Box>
      <Typography variant="h4" sx={{ mb: 0.5, fontWeight: 700 }}>
        Downloaded State
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
        Locally cached checkpoint data for fast re-inspection and diffing
      </Typography>

      {isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Failed to load cache entries
        </Alert>
      )}

      {diffSelection && (
        <Alert
          severity="info"
          sx={{ mb: 2 }}
          action={
            <Button
              color="inherit"
              size="small"
              onClick={() => setDiffSelection(null)}
            >
              Cancel
            </Button>
          }
        >
          Diff selection: <strong>{sourceLabel(diffSelection.sourcePath)}</strong>.
          Click <strong>Compare</strong> on a second checkpoint.
        </Alert>
      )}

      {entries && entries.length > 0 && (
        <Box sx={{ mb: 2, display: "flex", gap: 2 }}>
          <Chip
            label={`${entries.length} checkpoint${entries.length !== 1 ? "s" : ""}`}
            variant="outlined"
          />
          <Chip
            label={formatBytes(totalSize)}
            variant="outlined"
            color="primary"
          />
        </Box>
      )}

      {!isLoading && (!entries || entries.length === 0) && (
        <Paper
          sx={{
            p: 5,
            mb: 3,
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            textAlign: "center",
            bgcolor: "rgba(19, 47, 76, 0.5)",
            border: "1px dashed",
            borderColor: "rgba(80, 144, 211, 0.2)"
          }}
        >
          <CachedIcon sx={{ fontSize: 48, color: "rgba(80, 144, 211, 0.4)", mb: 2 }} />
          <Typography variant="h6" sx={{ color: "rgba(255,255,255,0.7)", mb: 1 }}>
            No cached checkpoints
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Inspect a checkpoint to download its state data locally. Cached data persists across sessions.
          </Typography>
        </Paper>
      )}

      {isLoading && (
        <Paper sx={{ overflow: "auto" }}>
          <Typography sx={{ p: 3 }} color="text.secondary">
            Loading...
          </Typography>
        </Paper>
      )}

      {entries && entries.length > 0 && (
      <Paper sx={{ overflow: "auto" }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Source</TableCell>
                <TableCell>Size</TableCell>
                <TableCell>Downloaded</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {entries.map(entry => {
                const isSelected =
                  diffSelection?.id === entry.id;
                return (
                  <TableRow
                    key={entry.id}
                    hover
                    sx={
                      isSelected
                        ? {
                            bgcolor: "rgba(25, 118, 210, 0.15)",
                            borderLeft: "3px solid",
                            borderColor: "primary.main"
                          }
                        : undefined
                    }
                  >
                    <TableCell
                      sx={{
                        fontFamily: '"JetBrains Mono", monospace',
                        fontSize: "0.8rem",
                        maxWidth: 500,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap"
                      }}
                      title={entry.sourcePath}
                    >
                      {entry.sourcePath}
                    </TableCell>
                    <TableCell>{formatBytes(entry.sizeBytes)}</TableCell>
                    <TableCell>{formatTime(entry.cachedAt)}</TableCell>
                    <TableCell>
                      <Box sx={{ display: "flex", gap: 1 }}>
                        <Button
                          size="small"
                          variant="outlined"
                          onClick={() => handleInspect(entry)}
                        >
                          Inspect
                        </Button>
                        <Button
                          size="small"
                          variant={
                            diffSelection && !isSelected
                              ? "contained"
                              : "outlined"
                          }
                          color={
                            isSelected
                              ? "primary"
                              : diffSelection
                              ? "warning"
                              : "secondary"
                          }
                          onClick={() => handleDiffClick(entry)}
                          disabled={isSelected}
                        >
                          {isSelected
                            ? "Selected"
                            : diffSelection
                            ? "Compare"
                            : "Diff"}
                        </Button>
                        <Button
                          size="small"
                          color="error"
                          variant="outlined"
                          onClick={() => handleDelete(entry.id)}
                          disabled={deleteMutation.isPending}
                        >
                          Delete
                        </Button>
                      </Box>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
      </Paper>
      )}
    </Box>
  );
}
