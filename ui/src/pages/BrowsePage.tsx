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
import MenuItem from "@mui/material/MenuItem";
import Select from "@mui/material/Select";
import FormControl from "@mui/material/FormControl";
import InputLabel from "@mui/material/InputLabel";
import InputAdornment from "@mui/material/InputAdornment";
import FolderIcon from "@mui/icons-material/Folder";
import CloudIcon from "@mui/icons-material/Cloud";
import StorageIcon from "@mui/icons-material/Storage";
import { useAppState } from "../context/AppStateContext";
import {
  discoverCheckpoints,
  discoverSavepoints,
  detectSources,
  type CheckpointInfo
} from "../api/client";

type SourceType = "local" | "docker" | "s3" | "gcs";

const SOURCE_TYPES: {
  value: SourceType;
  label: string;
  placeholder: string;
  icon: React.ReactNode;
  prefix?: string;
}[] = [
  {
    value: "local",
    label: "Local Filesystem",
    placeholder: "/path/to/checkpoints",
    icon: <FolderIcon fontSize="small" />
  },
  {
    value: "docker",
    label: "Docker Container",
    placeholder: "container-name/path/to/checkpoints",
    icon: <StorageIcon fontSize="small" />,
    prefix: "docker://"
  },
  {
    value: "s3",
    label: "AWS S3",
    placeholder: "bucket-name/prefix",
    icon: <CloudIcon fontSize="small" />,
    prefix: "s3://"
  },
  {
    value: "gcs",
    label: "Google Cloud Storage",
    placeholder: "bucket-name/prefix",
    icon: <CloudIcon fontSize="small" />,
    prefix: "gs://"
  }
];

function inferSourceType(path: string): SourceType {
  if (path.startsWith("docker://")) return "docker";
  if (path.startsWith("s3://") || path.startsWith("s3a://")) return "s3";
  if (path.startsWith("gs://")) return "gcs";
  return "local";
}

function stripPrefix(path: string, sourceType: SourceType): string {
  const config = SOURCE_TYPES.find(s => s.value === sourceType);
  if (config?.prefix && path.startsWith(config.prefix)) {
    return path.slice(config.prefix.length);
  }
  return path;
}

function addPrefix(path: string, sourceType: SourceType): string {
  const config = SOURCE_TYPES.find(s => s.value === sourceType);
  if (config?.prefix && !path.startsWith(config.prefix)) {
    return config.prefix + path;
  }
  return path;
}

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
  const [sourceType, setSourceType] = useState<SourceType>(() =>
    browse.manualPath ? inferSourceType(browse.manualPath) : "local"
  );
  const [pathInput, setPathInput] = useState(() =>
    browse.manualPath ? stripPrefix(browse.manualPath, inferSourceType(browse.manualPath)) : ""
  );

  const sourceConfig = SOURCE_TYPES.find(s => s.value === sourceType)!;

  const { data: detectedSources, isLoading: isDetecting } = useQuery({
    queryKey: ["detectSources"],
    queryFn: detectSources,
    enabled: sourceType === "docker",
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

  const doDiscover = useCallback(
    (fullPath: string) => {
      if (!fullPath) return;
      setBrowse({
        manualPath: fullPath,
        appPath: fullPath,
        currentLimit: INITIAL_LIMIT
      });
      currentLimitRef.current = INITIAL_LIMIT;
      setDiffSelection(null);
      discoverMutation.mutate({
        appPath: fullPath,
        totalLimit: INITIAL_LIMIT
      });
    },
    [setBrowse, setDiffSelection, discoverMutation]
  );

  const handleDiscover = useCallback(() => {
    const fullPath = addPrefix(pathInput, sourceType);
    doDiscover(fullPath);
  }, [pathInput, sourceType, doDiscover]);

  const handleSourceTypeChange = useCallback(
    (newType: SourceType) => {
      setSourceType(newType);
      setPathInput("");
      setBrowse({ manualPath: "" });
    },
    [setBrowse]
  );

  const handleDetectedSourceClick = useCallback(
    (path: string) => {
      setPathInput(stripPrefix(path, "docker"));
      doDiscover(path);
    },
    [doDiscover]
  );

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

  const handlePathChange = useCallback(
    (val: string) => {
      setPathInput(val);
      const fullPath = addPrefix(val, sourceType);
      setBrowse({ manualPath: fullPath });
      const inferred = inferSourceType(fullPath);
      if (inferred !== sourceType) {
        setSourceType(inferred);
        setPathInput(stripPrefix(fullPath, inferred));
      }
    },
    [sourceType, setBrowse]
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

        <Box sx={{ display: "flex", gap: 2, alignItems: "flex-start", mb: 2 }}>
          <FormControl size="small" sx={{ minWidth: 200 }}>
            <InputLabel id="source-type-label">Source</InputLabel>
            <Select
              labelId="source-type-label"
              value={sourceType}
              label="Source"
              onChange={e =>
                handleSourceTypeChange(e.target.value as SourceType)
              }
            >
              {SOURCE_TYPES.map(st => (
                <MenuItem key={st.value} value={st.value}>
                  <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                    {st.icon}
                    {st.label}
                  </Box>
                </MenuItem>
              ))}
            </Select>
          </FormControl>

          <TextField
            label="Path"
            placeholder={sourceConfig.placeholder}
            value={pathInput}
            onChange={e => handlePathChange(e.target.value)}
            onKeyDown={handleKeyDown}
            fullWidth
            size="small"
            InputProps={
              sourceConfig.prefix
                ? {
                    startAdornment: (
                      <InputAdornment position="start">
                        <Typography
                          variant="body2"
                          sx={{
                            fontFamily: '"JetBrains Mono", monospace',
                            color: "text.secondary",
                            fontSize: "0.85rem"
                          }}
                        >
                          {sourceConfig.prefix}
                        </Typography>
                      </InputAdornment>
                    )
                  }
                : undefined
            }
            sx={{
              "& input": { fontFamily: '"JetBrains Mono", monospace' }
            }}
          />
          <Button
            variant="contained"
            onClick={handleDiscover}
            disabled={!pathInput || discoverMutation.isPending}
            sx={{ whiteSpace: "nowrap", minWidth: 100 }}
          >
            Discover
          </Button>
        </Box>

        {sourceType === "docker" && isDetecting && (
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <CircularProgress size={16} />
            <Typography variant="body2" color="text.secondary">
              Scanning for running Flink containers...
            </Typography>
          </Box>
        )}
        {sourceType === "docker" &&
          detectedSources &&
          detectedSources.length > 0 && (
            <Box>
              <Typography
                variant="body2"
                color="text.secondary"
                sx={{ mb: 0.5 }}
              >
                Detected containers (click to discover):
              </Typography>
              <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap" }}>
                {detectedSources.map(source => {
                  const hasData = source.snapshotCount > 0;
                  const label = hasData
                    ? `${source.label} (${source.snapshotCount})`
                    : source.label;
                  return (
                    <Chip
                      key={source.path}
                      label={label}
                      size="small"
                      color={hasData ? "success" : "default"}
                      variant={
                        browse.manualPath === source.path
                          ? "filled"
                          : hasData
                          ? "filled"
                          : "outlined"
                      }
                      onClick={() => handleDetectedSourceClick(source.path)}
                      clickable
                    />
                  );
                })}
              </Box>
            </Box>
          )}
        {sourceType === "docker" &&
          !isDetecting &&
          detectedSources &&
          detectedSources.length === 0 && (
            <Typography variant="body2" color="text.secondary">
              No running Flink containers detected.
            </Typography>
          )}
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
