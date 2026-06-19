import React from "react";
import Box from "@mui/material/Box";

interface JsonViewerProps {
  data: unknown;
}

const COLORS = {
  key: "#90caf9",
  string: "#ffa726",
  number: "#66bb6a",
  boolean: "#ce93d8",
  null: "#f44336",
  bracket: "#b0bec5"
};

function syntaxHighlight(json: string): React.ReactNode[] {
  const nodes: React.ReactNode[] = [];
  let idx = 0;

  const regex =
    /("(\\u[\da-fA-F]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?)/g;
  let lastIndex = 0;
  let match: RegExpExecArray | null;

  while ((match = regex.exec(json)) !== null) {
    if (match.index > lastIndex) {
      nodes.push(
        <span key={idx++} style={{ color: COLORS.bracket }}>
          {json.slice(lastIndex, match.index)}
        </span>
      );
    }

    const token = match[0];
    let color: string;

    if (/^"/.test(token)) {
      if (/:$/.test(match[0])) {
        color = COLORS.key;
      } else {
        color = COLORS.string;
      }
    } else if (/true|false/.test(token)) {
      color = COLORS.boolean;
    } else if (/null/.test(token)) {
      color = COLORS.null;
    } else {
      color = COLORS.number;
    }

    nodes.push(
      <span key={idx++} style={{ color }}>
        {token}
      </span>
    );
    lastIndex = regex.lastIndex;
  }

  if (lastIndex < json.length) {
    nodes.push(
      <span key={idx++} style={{ color: COLORS.bracket }}>
        {json.slice(lastIndex)}
      </span>
    );
  }

  return nodes;
}

export default function JsonViewer({ data }: JsonViewerProps) {
  const formatted = JSON.stringify(data, null, 2);

  return (
    <Box
      component="pre"
      role="code"
      sx={{
        fontFamily: '"JetBrains Mono", monospace',
        fontSize: "0.8rem",
        lineHeight: 1.5,
        p: 2,
        m: 0,
        bgcolor: "rgba(0, 0, 0, 0.3)",
        borderRadius: 1,
        overflow: "auto",
        maxHeight: 500,
        whiteSpace: "pre-wrap",
        wordBreak: "break-all"
      }}
    >
      {syntaxHighlight(formatted)}
    </Box>
  );
}
