import { useState, useEffect, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import TextField from "@mui/material/TextField";
import Button from "@mui/material/Button";
import FormControl from "@mui/material/FormControl";
import InputLabel from "@mui/material/InputLabel";
import Select, { SelectChangeEvent } from "@mui/material/Select";
import MenuItem from "@mui/material/MenuItem";
import Paper from "@mui/material/Paper";
import CircularProgress from "@mui/material/CircularProgress";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import Chip from "@mui/material/Chip";
import CompareArrowsIcon from "@mui/icons-material/CompareArrows";
import { useDiscoverOperators } from "../hooks/useOperators";
import { useDiffKeyed, useDiffBroadcast } from "../hooks/useDiff";
import DiffView from "../components/DiffView";
import ErrorAlert from "../components/ErrorAlert";
import { useAppState } from "../context/AppStateContext";

type DiffType = "keyed" | "broadcast";

export default function DiffPage() {
  const [searchParams] = useSearchParams();
  const {
    lastDiffPath1,
    setLastDiffPath1,
    lastDiffPath2,
    setLastDiffPath2,
    discoveredOperators,
    setDiscoveredOperators
  } = useAppState();

  const initialPath1 = searchParams.get("path1") || lastDiffPath1;
  const initialPath2 = searchParams.get("path2") || lastDiffPath2;
  const [path1, setPath1Local] = useState(initialPath1);
  const [path2, setPath2Local] = useState(initialPath2);
  const [diffType, setDiffType] = useState<DiffType>("keyed");
  const [selectedOperator, setSelectedOperator] = useState("");
  const [selectedStateName, setSelectedStateName] = useState("");
  const [keyFilter, setKeyFilter] = useState("");

  const setPath1 = useCallback(
    (p: string) => {
      setPath1Local(p);
      setLastDiffPath1(p);
    },
    [setLastDiffPath1]
  );

  const setPath2 = useCallback(
    (p: string) => {
      setPath2Local(p);
      setLastDiffPath2(p);
    },
    [setLastDiffPath2]
  );

  const discoverMutation = useDiscoverOperators();
  const diffKeyedMutation = useDiffKeyed();
  const diffBroadcastMutation = useDiffBroadcast();

  const activeMutation =
    diffType === "keyed" ? diffKeyedMutation : diffBroadcastMutation;

  useEffect(() => {
    const paramPath1 = searchParams.get("path1");
    const paramPath2 = searchParams.get("path2");
    if (paramPath1) setPath1(paramPath1);
    if (paramPath2) setPath2(paramPath2);
  }, [searchParams, setPath1, setPath2]);

  const handleDiscoverOperators = useCallback(() => {
    if (!path1) return;
    setSelectedOperator("");
    discoverMutation.mutate(path1, {
      onSuccess: data => {
        setDiscoveredOperators(data.operators);
      }
    });
  }, [path1, discoverMutation, setDiscoveredOperators]);

  const handleCompare = () => {
    if (!path1 || !path2 || !selectedOperator) return;

    if (diffType === "keyed") {
      diffKeyedMutation.mutate({
        path1,
        path2,
        operatorUid: selectedOperator,
        keyFilter: keyFilter || undefined
      });
    } else {
      diffBroadcastMutation.mutate({
        path1,
        path2,
        operatorUid: selectedOperator,
        stateName: selectedStateName || undefined,
        keyFilter: keyFilter || undefined
      });
    }
  };

  const operators = discoveredOperators;
  const keyedOperators = operators.filter(
    op => op.keyedStates && op.keyedStates.length > 0
  );
  const operatorStateOperators = operators.filter(
    op => op.operatorStates && op.operatorStates.length > 0
  );
  const visibleOperators =
    diffType === "keyed" ? keyedOperators : operatorStateOperators;

  const selectedOp = operators.find(op => op.uid === selectedOperator);
  const availableStateNames =
    diffType === "broadcast" && selectedOp ? selectedOp.operatorStates : [];

  const result = activeMutation.data?.data;

  return (
    <Box>
      <Typography variant="h4" sx={{ mb: 0.5, fontWeight: 700 }}>
        Compare Checkpoints
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
        Select two checkpoints to compare state side-by-side
      </Typography>

      <Paper sx={{ p: 3, mb: 3 }}>
        <Box sx={{ display: "flex", gap: 2, mb: 2 }}>
          <TextField
            label="Checkpoint 1"
            placeholder="/checkpoints/job-id/chk-123"
            value={path1}
            onChange={e => setPath1(e.target.value)}
            fullWidth
            size="small"
            sx={{ "& input": { fontFamily: '"JetBrains Mono", monospace' } }}
          />
          <TextField
            label="Checkpoint 2"
            placeholder="/checkpoints/job-id/chk-456"
            value={path2}
            onChange={e => setPath2(e.target.value)}
            fullWidth
            size="small"
            sx={{ "& input": { fontFamily: '"JetBrains Mono", monospace' } }}
          />
        </Box>

        <Box sx={{ mb: 2 }}>
          <Button
            variant="outlined"
            onClick={handleDiscoverOperators}
            disabled={!path1 || discoverMutation.isPending}
            sx={{ whiteSpace: "nowrap" }}
          >
            {discoverMutation.isPending ? (
              <CircularProgress size={20} sx={{ mr: 1 }} />
            ) : null}
            Discover Operators from Checkpoint 1
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
              Found {operators.length} operator(s):
            </Typography>
            <Box sx={{ display: "flex", gap: 0.5, flexWrap: "wrap" }}>
              {operators.map(op => (
                <Chip
                  key={op.uid}
                  label={op.name || op.uid}
                  size="small"
                  variant="outlined"
                  color="primary"
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
            flexWrap: "wrap"
          }}
        >
          <ToggleButtonGroup
            value={diffType}
            exclusive
            onChange={(_e, val) => {
              if (val) {
                setDiffType(val);
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
            <InputLabel id="diff-operator-label">Operator</InputLabel>
            <Select
              labelId="diff-operator-label"
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
                <MenuItem key={op.uid} value={op.uid}>
                  {op.name || op.uid}
                </MenuItem>
              ))}
            </Select>
          </FormControl>

          {diffType === "broadcast" && availableStateNames.length > 0 && (
            <FormControl sx={{ minWidth: 250 }}>
              <InputLabel id="diff-state-label">State Name</InputLabel>
              <Select
                labelId="diff-state-label"
                value={selectedStateName}
                label="State Name"
                onChange={(e: SelectChangeEvent) =>
                  setSelectedStateName(e.target.value)
                }
                size="small"
              >
                {availableStateNames.map(name => (
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

          <Button
            variant="contained"
            onClick={handleCompare}
            disabled={
              !path1 ||
              !path2 ||
              !selectedOperator ||
              (diffType === "broadcast" && !selectedStateName) ||
              activeMutation.isPending
            }
            sx={{ whiteSpace: "nowrap" }}
          >
            Compare
          </Button>
        </Box>
      </Paper>

      {!result && !activeMutation.isPending && !activeMutation.isError && operators.length === 0 && (
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
          <CompareArrowsIcon sx={{ fontSize: 48, color: "rgba(80, 144, 211, 0.4)", mb: 2 }} />
          <Typography variant="h6" sx={{ color: "rgba(255,255,255,0.7)", mb: 1 }}>
            No comparison loaded
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Enter two checkpoint paths, discover operators, and click Compare to see differences
          </Typography>
        </Paper>
      )}

      {activeMutation.isPending && (
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, my: 3 }}>
          <CircularProgress size={24} />
          <Typography>Comparing checkpoints...</Typography>
        </Box>
      )}

      {activeMutation.isError && (
        <ErrorAlert
          error={
            activeMutation.error instanceof Error
              ? activeMutation.error
              : new Error("Failed to compare checkpoints")
          }
          fallbackMessage="Failed to compare checkpoints"
        />
      )}

      {result && (
        <Box>
          <Typography variant="h6" sx={{ mb: 2 }}>
            {result.operatorName}
          </Typography>
          <DiffView
            diff={result}
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
