import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation, useQuery } from "@tanstack/react-query";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import TextField from "@mui/material/TextField";
import Button from "@mui/material/Button";
import Paper from "@mui/material/Paper";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import TableSortLabel from "@mui/material/TableSortLabel";
import Chip from "@mui/material/Chip";
import CircularProgress from "@mui/material/CircularProgress";
import Alert from "@mui/material/Alert";
import { useAppState } from "../context/AppStateContext";
import {
  discoverCheckpoints,
  discoverSavepoints,
  detectSources,
  type CheckpointInfo,
  type DetectedSource
} from "../api/client";

const INITIAL_LIMIT = 20;
const MAX_LIMIT = 1000;

type SortField = "type" | "jobId" | "snapshot" | "modified";
type SortDirection = "asc" | "desc";

function useDiscoverSnapshots() {
  return useMutation({
    mutationFn: async ({
      appPath,
      totalLimit
    }: {
      appPath: string;
      totalLimit: number;
    }): Promise<CheckpointInfo[]> => {
      const [cpResults, spResults] = await Promise.all([
        discoverCheckpoints(appPath, undefined, totalLimit).catch(() => []),
        discoverSavepoints(appPath, totalLimit).catch(() => [])
      ]);

      const merged = [...cpResults, ...spResults];
      merged.sort((a, b) => b.modificationTime - a.modificationTime);
      return merged.slice(0, totalLimit);
    }
  });
}

export default function BrowsePage() {
  const navigate = useNavigate();
  const { browse, setBrowse, diffSelection, setDiffSelection } = useAppState();
  const currentLimitRef = useRef(browse.currentLimit);

  const [sortField, setSortField] = useState<SortField>("modified");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");

  const { data: detectedSources } = useQuery({
    queryKey: ["detectSources"],
    queryFn: detectSources,
    staleTime: 30_000,
    retry: false
  });

  const discoverMutation = useDiscoverSnapshots();

  const mutationData = discoverMutation.data;
  useEffect(() => {
    if (mutationData) {
      setBrowse({ checkpoints: mutationData });
    }
  }, [mutationData, setBrowse]);

  const handleDiscover = useCallback(() => {
    if (!browse.manualPath) return;
    setBrowse({
      appPath: browse.manualPath,
      currentLimit: INITIAL_LIMIT
    });
    currentLimitRef.current = INITIAL_LIMIT;
    setDiffSelection(null);
    discoverMutation.mutate({
      appPath: browse.manualPath,
      totalLimit: INITIAL_LIMIT
    });
  }, [browse.manualPath, setBrowse, setDiffSelection, discoverMutation]);

  const handleLoadMore = useCallback(() => {
    if (!browse.appPath) return;
    const nextLimit = Math.min(currentLimitRef.current * 2, MAX_LIMIT);
    currentLimitRef.current = nextLimit;
    setBrowse({ currentLimit: nextLimit });
    discoverMutation.mutate({
      appPath: browse.appPath,
      totalLimit: nextLimit
    });
  }, [browse.appPath, setBrowse, discoverMutation]);

  const handleRefresh = useCallback(() => {
    if (!browse.appPath) return;
    discoverMutation.mutate({
      appPath: browse.appPath,
      totalLimit: currentLimitRef.current
    });
  }, [browse.appPath, discoverMutation]);

  const handleSelectCheckpoint = useCallback(
    (cp: CheckpointInfo) => {
      navigate(`/inspect?path=${encodeURIComponent(cp.path)}`);
    },
    [navigate]
  );

  const handleDiffClick = useCallback(
    (cp: CheckpointInfo) => {
      if (!diffSelection) {
        setDiffSelection({
          path1: cp.path,
          jobId1: cp.shortJobId,
          displayLabel1: cp.displayLabel
        });
      } else {
        navigate(
          `/diff?path1=${encodeURIComponent(
            diffSelection.path1
          )}&path2=${encodeURIComponent(cp.path)}`
        );
        setDiffSelection(null);
      }
    },
    [diffSelection, setDiffSelection, navigate]
  );

  const handleCancelDiff = useCallback(() => {
    setDiffSelection(null);
  }, [setDiffSelection]);

  const handleSort = useCallback(
    (field: SortField) => {
      if (sortField === field) {
        setSortDirection(prev => (prev === "asc" ? "desc" : "asc"));
      } else {
        setSortField(field);
        setSortDirection(field === "modified" ? "desc" : "asc");
      }
    },
    [sortField]
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Enter") handleDiscover();
    },
    [handleDiscover]
  );

  const rawCheckpoints = mutationData ?? browse.checkpoints;
  const checkpoints = useMemo(() => {
    if (!rawCheckpoints || rawCheckpoints.length === 0) return rawCheckpoints;
    const sorted = [...rawCheckpoints];
    sorted.sort((a, b) => {
      let cmp = 0;
      switch (sortField) {
        case "type":
          cmp = (a.type ?? "checkpoint").localeCompare(b.type ?? "checkpoint");
          break;
        case "jobId":
          cmp = a.shortJobId.localeCompare(b.shortJobId);
          break;
        case "snapshot":
          cmp = (a.displayLabel ?? "").localeCompare(b.displayLabel ?? "");
          break;
        case "modified":
          cmp = a.modificationTime - b.modificationTime;
          break;
      }
      return sortDirection === "asc" ? cmp : -cmp;
    });
    return sorted;
  }, [rawCheckpoints, sortField, sortDirection]);

  const resultCount = checkpoints?.length ?? 0;
  const canLoadMore =
    resultCount > 0 &&
    resultCount >= currentLimitRef.current &&
    currentLimitRef.current < MAX_LIMIT;

  return (
    <Box>
      <Typography variant="h4" sx={{ mb: 3 }}>
        Browse Snapshots
      </Typography>

      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="h6" sx={{ mb: 2 }}>
          Discover Snapshots
        </Typography>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Enter a path to scan for checkpoints and savepoints. Supported
          schemes: local filesystem paths, s3://, gs://, docker://container/path
        </Typography>
        {detectedSources && detectedSources.length > 0 && (
          <Box sx={{ mb: 2 }}>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 0.5 }}>
              Detected sources:
            </Typography>
            <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap" }}>
              {detectedSources.map(source => (
                <Chip
                  key={source.path}
                  label={source.label}
                  size="small"
                  color="primary"
                  variant={browse.manualPath === source.path ? "filled" : "outlined"}
                  onClick={() => setBrowse({ manualPath: source.path })}
                  clickable
                />
              ))}
            </Box>
          </Box>
        )}
        <Box sx={{ display: "flex", gap: 2, alignItems: "flex-end" }}>
          <TextField
            label="Snapshot path"
            placeholder="/opt/flink/checkpoints/my-job/"
            value={browse.manualPath}
            onChange={e => setBrowse({ manualPath: e.target.value })}
            onKeyDown={handleKeyDown}
            fullWidth
            size="small"
            sx={{
              "& input": { fontFamily: '"JetBrains Mono", monospace' }
            }}
          />
          <Button
            variant="contained"
            onClick={handleDiscover}
            disabled={!browse.manualPath || discoverMutation.isPending}
            sx={{ whiteSpace: "nowrap" }}
          >
            Discover
          </Button>
        </Box>
      </Paper>

      {discoverMutation.isPending && (
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, my: 3 }}>
          <CircularProgress size={24} />
          <Typography>Discovering snapshots at {browse.appPath}...</Typography>
        </Box>
      )}

      {discoverMutation.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {discoverMutation.error instanceof Error
            ? discoverMutation.error.message
            : "Failed to discover snapshots"}
        </Alert>
      )}

      {diffSelection && (
        <Alert
          severity="info"
          sx={{ mb: 2 }}
          action={
            <Button color="inherit" size="small" onClick={handleCancelDiff}>
              Cancel
            </Button>
          }
        >
          Snapshot 1 selected: <strong>{diffSelection.displayLabel1}</strong> (
          {diffSelection.jobId1}). Click <strong>Diff</strong> on a second
          snapshot to compare.
        </Alert>
      )}

      {checkpoints && (
        <Paper sx={{ overflow: "auto" }}>
          <Box
            sx={{
              p: 2,
              pb: 1,
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between"
            }}
          >
            <Typography variant="h6">Snapshots ({resultCount})</Typography>
            <Button
              size="small"
              variant="outlined"
              onClick={handleRefresh}
              disabled={discoverMutation.isPending}
            >
              Refresh
            </Button>
          </Box>
          {resultCount === 0 ? (
            <Typography sx={{ p: 2 }} color="text.secondary">
              No snapshots found at {browse.appPath}
            </Typography>
          ) : (
            <>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>#</TableCell>
                    <TableCell
                      sortDirection={
                        sortField === "type" ? sortDirection : false
                      }
                    >
                      <TableSortLabel
                        active={sortField === "type"}
                        direction={sortField === "type" ? sortDirection : "asc"}
                        onClick={() => handleSort("type")}
                      >
                        Type
                      </TableSortLabel>
                    </TableCell>
                    <TableCell
                      sortDirection={
                        sortField === "jobId" ? sortDirection : false
                      }
                    >
                      <TableSortLabel
                        active={sortField === "jobId"}
                        direction={
                          sortField === "jobId" ? sortDirection : "asc"
                        }
                        onClick={() => handleSort("jobId")}
                      >
                        Job ID
                      </TableSortLabel>
                    </TableCell>
                    <TableCell
                      sortDirection={
                        sortField === "snapshot" ? sortDirection : false
                      }
                    >
                      <TableSortLabel
                        active={sortField === "snapshot"}
                        direction={
                          sortField === "snapshot" ? sortDirection : "asc"
                        }
                        onClick={() => handleSort("snapshot")}
                      >
                        Snapshot
                      </TableSortLabel>
                    </TableCell>
                    <TableCell
                      sortDirection={
                        sortField === "modified" ? sortDirection : false
                      }
                    >
                      <TableSortLabel
                        active={sortField === "modified"}
                        direction={
                          sortField === "modified" ? sortDirection : "desc"
                        }
                        onClick={() => handleSort("modified")}
                      >
                        Modified
                      </TableSortLabel>
                    </TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {checkpoints.map((cp, idx) => {
                    const isSelected = diffSelection?.path1 === cp.path;
                    const isSavepoint =
                      cp.type === "savepoint" ||
                      cp.displayLabel?.startsWith("savepoint-");
                    return (
                      <TableRow
                        key={cp.path}
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
                        <TableCell>{idx + 1}</TableCell>
                        <TableCell>
                          <Chip
                            size="small"
                            label={isSavepoint ? "Savepoint" : "Checkpoint"}
                            color={isSavepoint ? "info" : "default"}
                            variant="outlined"
                          />
                        </TableCell>
                        <TableCell
                          sx={{
                            fontFamily: '"JetBrains Mono", monospace',
                            fontSize: "0.8rem",
                            color: isSavepoint
                              ? "text.disabled"
                              : "text.primary"
                          }}
                        >
                          {cp.shortJobId}
                        </TableCell>
                        <TableCell
                          sx={{ fontFamily: '"JetBrains Mono", monospace' }}
                        >
                          {cp.displayLabel ?? `chk-${cp.chkNumber}`}
                        </TableCell>
                        <TableCell>{cp.formattedTime}</TableCell>
                        <TableCell>
                          <Chip
                            size="small"
                            label={cp.valid ? "Valid" : "Invalid"}
                            color={cp.valid ? "success" : "error"}
                            variant="outlined"
                          />
                        </TableCell>
                        <TableCell>
                          <Box sx={{ display: "flex", gap: 1 }}>
                            <Button
                              size="small"
                              variant="outlined"
                              onClick={() => handleSelectCheckpoint(cp)}
                              disabled={!cp.valid}
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
                              onClick={() => handleDiffClick(cp)}
                              disabled={!cp.valid || isSelected}
                            >
                              {isSelected
                                ? "Selected"
                                : diffSelection
                                ? "Compare"
                                : "Diff"}
                            </Button>
                          </Box>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
              {canLoadMore && (
                <Box sx={{ p: 2, display: "flex", justifyContent: "center" }}>
                  <Button
                    variant="outlined"
                    onClick={handleLoadMore}
                    disabled={discoverMutation.isPending}
                  >
                    Load More (showing {resultCount}, limit{" "}
                    {currentLimitRef.current})
                  </Button>
                </Box>
              )}
            </>
          )}
        </Paper>
      )}
    </Box>
  );
}
