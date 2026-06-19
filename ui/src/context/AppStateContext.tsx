import { createContext, useContext, useState, useCallback } from "react";
import type { CheckpointInfo, OperatorInfo } from "../api/client";

interface BrowseState {
  manualPath: string;
  appPath: string;
  checkpoints: CheckpointInfo[] | null;
  currentLimit: number;
}

interface DiffSelection {
  path1: string;
  jobId1: string;
  displayLabel1: string;
}

interface AppState {
  browse: BrowseState;
  setBrowse: (update: Partial<BrowseState>) => void;
  resetBrowse: () => void;

  diffSelection: DiffSelection | null;
  setDiffSelection: (sel: DiffSelection | null) => void;

  lastInspectPath: string;
  setLastInspectPath: (path: string) => void;

  lastDiffPath1: string;
  setLastDiffPath1: (path: string) => void;
  lastDiffPath2: string;
  setLastDiffPath2: (path: string) => void;

  discoveredOperators: OperatorInfo[];
  setDiscoveredOperators: (ops: OperatorInfo[]) => void;
}

const INITIAL_LIMIT = 20;

const defaultBrowse: BrowseState = {
  manualPath: "",
  appPath: "",
  checkpoints: null,
  currentLimit: INITIAL_LIMIT
};

const AppStateContext = createContext<AppState | null>(null);

export function AppStateProvider({ children }: { children: React.ReactNode }) {
  const [browse, setBrowseState] = useState<BrowseState>(defaultBrowse);
  const [diffSelection, setDiffSelection] = useState<DiffSelection | null>(
    null
  );
  const [lastInspectPath, setLastInspectPath] = useState("");
  const [lastDiffPath1, setLastDiffPath1] = useState("");
  const [lastDiffPath2, setLastDiffPath2] = useState("");
  const [discoveredOperators, setDiscoveredOperators] = useState<
    OperatorInfo[]
  >([]);

  const setBrowse = useCallback((update: Partial<BrowseState>) => {
    setBrowseState(prev => ({ ...prev, ...update }));
  }, []);

  const resetBrowse = useCallback(() => {
    setBrowseState(defaultBrowse);
  }, []);

  return (
    <AppStateContext.Provider
      value={{
        browse,
        setBrowse,
        resetBrowse,
        diffSelection,
        setDiffSelection,
        lastInspectPath,
        setLastInspectPath,
        lastDiffPath1,
        setLastDiffPath1,
        lastDiffPath2,
        setLastDiffPath2,
        discoveredOperators,
        setDiscoveredOperators
      }}
    >
      {children}
    </AppStateContext.Provider>
  );
}

export function useAppState(): AppState {
  const ctx = useContext(AppStateContext);
  if (!ctx) {
    throw new Error("useAppState must be used within AppStateProvider");
  }
  return ctx;
}
