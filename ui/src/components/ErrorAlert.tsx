import { useState } from "react";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Collapse from "@mui/material/Collapse";
import { ApiError } from "../api/client";

interface ErrorAlertProps {
  error: Error;
  fallbackMessage?: string;
}

export default function ErrorAlert({
  error,
  fallbackMessage
}: ErrorAlertProps) {
  const [showDetails, setShowDetails] = useState(false);
  const message =
    error.message || fallbackMessage || "An unexpected error occurred";
  const stackTrace = error instanceof ApiError ? error.stackTrace : undefined;

  return (
    <Box sx={{ mb: 2 }}>
      <Alert
        severity="error"
        action={
          stackTrace ? (
            <Button
              color="inherit"
              size="small"
              onClick={() => setShowDetails(prev => !prev)}
            >
              {showDetails ? "Hide Details" : "Show Details"}
            </Button>
          ) : undefined
        }
      >
        {message}
      </Alert>
      {stackTrace && (
        <Collapse in={showDetails}>
          <Box
            component="pre"
            sx={{
              mt: 1,
              p: 2,
              bgcolor: "grey.900",
              color: "error.light",
              borderRadius: 1,
              fontSize: "0.75rem",
              fontFamily: '"JetBrains Mono", monospace',
              overflow: "auto",
              maxHeight: 400,
              whiteSpace: "pre-wrap",
              wordBreak: "break-all"
            }}
          >
            {stackTrace}
          </Box>
        </Collapse>
      )}
    </Box>
  );
}
