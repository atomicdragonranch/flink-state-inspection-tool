import { useState, useEffect, useCallback, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import TextField from "@mui/material/TextField";
import Button from "@mui/material/Button";
import FormControl from "@mui/material/FormControl";
import FormControlLabel from "@mui/material/FormControlLabel";
import Checkbox from "@mui/material/Checkbox";
import InputLabel from "@mui/material/InputLabel";
import Select, { SelectChangeEvent } from "@mui/material/Select";
import MenuItem from "@mui/material/MenuItem";
import Paper from "@mui/material/Paper";
import CircularProgress from "@mui/material/CircularProgress";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import Chip from "@mui/material/Chip";
import { useDiscoverOperators } from "../hooks/useOperators";
import { useInspectKeyed, useInspectBroadcast } from "../hooks/useInspect";
import StateTable from "../components/StateTable";
import ErrorAlert from "../components/ErrorAlert";
import { useAppState } from "../context/AppStateContext";
import type { OperatorInfo } from "../api/client";

type InspectType = "keyed" | "broadcast";

function operatorLabel(op: OperatorInfo): string {
  const base = op.name || op.uid.substring(0, 12) + "...";
  const parts: string[] = [];
  if (op.keyedStates.length > 0) {
    const count = op.keyedStateEntryCount;
    parts.push(count > 0 ? "has data" : count === 0 ? "empty" : "keyed");
  }
  if (op.operatorStates.length > 0) {
    parts.push(op.operatorStates.join(", "));
  }
  return parts.length > 0 ? `${base} (${parts.join(" + ")})` : base;
}

export default function InspectPage() {
  const [searchParams] = useSearchParams();
  const {
    lastInspectPath,
    setLastInspectPath,
    discoveredOperators,
    setDiscoveredOperators
  } = useAppState();

  const initialPath = searchParams.get("path") || lastInspectPath;
  const [path, setPathLocal] = useState(initialPath);
  const [inspectType, setInspectType] = useState<InspectType>("keyed");
  const [selectedOperator, setSelectedOperator] = useState("");
  const [selectedStateName, setSelectedStateName] = useState("");
  const [keyFilter, setKeyFilter] = useState("");
  const [keysOnly, setKeysOnly] = useState(false);

  const setPath = useCallback(
    (newPath: string) => {
      setPathLocal(newPath);
      setLastInspectPath(newPath);
    },
    [setLastInspectPath]
  );

  const discoverMutation = useDiscoverOperators();
  const inspectKeyedMutation = useInspectKeyed();
  const inspectBroadcastMutation = useInspectBroadcast();

  const activeMutation =
    inspectType === "keyed" ? inspectKeyedMutation : inspectBroadcastMutation;

  useEffect(() => {
    const paramPath = searchParams.get("path");
    if (paramPath) {
      setPath(paramPath);
      if (discoveredOperators.length === 0 && !discoverMutation.isPending) {
        discoverMutation.mutate(paramPath, {
          onSuccess: data => {
            setDiscoveredOperators(data.operators);
          }
        });
      }
    }
  }, [searchParams]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleDiscoverOperators = useCallback(() => {
    if (!path) return;
    setSelectedOperator("");
    setSelectedStateName("");
    discoverMutation.mutate(path, {
      onSuccess: data => {
        setDiscoveredOperators(data.operators);
      }
    });
  }, [path, discoverMutation, setDiscoveredOperators]);

  const operators = discoveredOperators;
  const keyedOperators = operators.filter(
    op => op.keyedStates && op.keyedStates.length > 0
  );
  const operatorStateOperators = operators.filter(
    op => op.operatorStates && op.operatorStates.length > 0
  );
  const visibleOperators =
    inspectType === "keyed" ? keyedOperators : operatorStateOperators;

  const selectedOp = useMemo(
    () => operators.find(op => op.uid === selectedOperator),
    [operators, selectedOperator]
  );

  const stateNames = useMemo(() => {
    if (inspectType !== "broadcast" || !selectedOp) return [];
    return selectedOp.operatorStates;
  }, [inspectType, selectedOp]);

  useEffect(() => {
    if (stateNames.length === 1) {
      setSelectedStateName(stateNames[0]);
    } else {
      setSelectedStateName("");
    }
  }, [stateNames]);

  const handleInspect = () => {
    if (!path || !selectedOperator) return;
    const inspectPath = path;

    if (inspectType === "keyed") {
      inspectKeyedMutation.mutate({
        path: inspectPath,
        operatorUid: selectedOperator,
        keyFilter: keyFilter || undefined,
        keysOnly: keysOnly || undefined
      });
    } else {
      if (!selectedStateName) return;
      inspectBroadcastMutation.mutate({
        path: inspectPath,
        operatorUid: selectedOperator,
        stateName: selectedStateName,
        keyFilter: keyFilter || undefined
      });
    }
  };

  const singleResult = activeMutation.data?.data;

  return (
    <Box>
      <Typography variant="h4" sx={{ mb: 3 }}>
        Inspect State
      </Typography>

      <Paper sx={{ p: 3, mb: 3 }}>
        <Box sx={{ display: "flex", gap: 2, alignItems: "flex-end", mb: 2 }}>
          <TextField
            label="Checkpoint / Savepoint Path"
            placeholder="/opt/flink/checkpoints/job-id/chk-123"
            value={path}
            onChange={e => setPath(e.target.value)}
            fullWidth
            size="small"
            sx={{
              "& input": { fontFamily: '"JetBrains Mono", monospace' }
            }}
          />
          <Button
            variant="outlined"
            onClick={handleDiscoverOperators}
            disabled={!path || discoverMutation.isPending}
            sx={{ whiteSpace: "nowrap" }}
          >
            {discoverMutation.isPending ? (
              <CircularProgress size={20} sx={{ mr: 1 }} />
            ) : null}
            Discover Operators
          </Button>
        </Box>

        {discoverMutation.isError && (
          <ErrorAlert
            error={
              discoverMutation.error instanceof Error
                ? discoverMutation.error
                : new Error("Failed to discover operators")
            }
          />
        )}

        {operators.length > 0 && (
          <Box sx={{ mb: 2 }}>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
              Found {operators.length} operator(s) in checkpoint:
            </Typography>
            <Box sx={{ display: "flex", gap: 0.5, flexWrap: "wrap" }}>
              {operators.map(op => (
                <Chip
                  key={op.uid}
                  label={operatorLabel(op)}
                  size="small"
                  variant={op.keyedStateEntryCount > 0 ? "filled" : "outlined"}
                  color={op.keyedStateEntryCount > 0 ? "success" : "default"}
                />
              ))}
            </Box>
          </Box>
        )}

        <Box
          sx={{
            display: "flex",
            gap: 2,
            alignItems: "flex-end",
            flexWrap: "wrap",
            mb: 2
          }}
        >
          <ToggleButtonGroup
            value={inspectType}
            exclusive
            onChange={(_e, val) => {
              if (val) {
                setInspectType(val);
                setSelectedOperator("");
                setSelectedStateName("");
              }
            }}
            size="small"
          >
            <ToggleButton value="keyed">Keyed State</ToggleButton>
            <ToggleButton value="broadcast">Operator State</ToggleButton>
          </ToggleButtonGroup>

          <FormControl sx={{ minWidth: 350 }}>
            <InputLabel id="operator-select-label">Operator</InputLabel>
            <Select
              labelId="operator-select-label"
              value={selectedOperator}
              label="Operator"
              onChange={(e: SelectChangeEvent) => {
                setSelectedOperator(e.target.value);
                setSelectedStateName("");
              }}
              size="small"
              disabled={visibleOperators.length === 0}
            >
              {visibleOperators.map(op => (
                <MenuItem key={op.uid} value={op.uid}
                  sx={op.keyedStateEntryCount > 0 ? { fontWeight: "bold" } : undefined}
                >
                  {operatorLabel(op)}
                </MenuItem>
              ))}
            </Select>
          </FormControl>

          {inspectType === "broadcast" && stateNames.length > 1 && (
            <FormControl sx={{ minWidth: 200 }}>
              <InputLabel id="statename-select-label">State Name</InputLabel>
              <Select
                labelId="statename-select-label"
                value={selectedStateName}
                label="State Name"
                onChange={(e: SelectChangeEvent) =>
                  setSelectedStateName(e.target.value)
                }
                size="small"
              >
                {stateNames.map(name => (
                  <MenuItem key={name} value={name}>
                    {name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          )}

          <TextField
            label="Key Filter (optional)"
            placeholder="filter by key substring"
            value={keyFilter}
            onChange={e => setKeyFilter(e.target.value)}
            size="small"
            sx={{ minWidth: 200 }}
          />

          {inspectType === "keyed" && (
            <FormControlLabel
              control={
                <Checkbox
                  checked={keysOnly}
                  onChange={e => setKeysOnly(e.target.checked)}
                  size="small"
                />
              }
              label="Keys only"
            />
          )}

          <Button
            variant="contained"
            onClick={handleInspect}
            disabled={
              !path ||
              !selectedOperator ||
              (inspectType === "broadcast" && !selectedStateName) ||
              activeMutation.isPending
            }
            sx={{ whiteSpace: "nowrap" }}
          >
            Read State
          </Button>
        </Box>
      </Paper>

      {activeMutation.isPending && (
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, my: 3 }}>
          <CircularProgress size={24} />
          <Typography>Reading state from checkpoint...</Typography>
        </Box>
      )}

      {activeMutation.isError && (
        <ErrorAlert
          error={
            activeMutation.error instanceof Error
              ? activeMutation.error
              : new Error("Failed to read state")
          }
          fallbackMessage="Failed to read state"
        />
      )}

      {singleResult && (
        <Box>
          <Typography variant="h6" sx={{ mb: 1 }}>
            {singleResult.operatorName || singleResult.operatorUid} (
            {singleResult.entryCount} entries)
          </Typography>
          <StateTable
            entries={singleResult.entries}
            columns={singleResult.columns}
            partialRead={activeMutation.data?.partialRead ?? undefined}
            partialReadCause={
              activeMutation.data?.partialReadCause ?? undefined
            }
          />
        </Box>
      )}
    </Box>
  );
}
