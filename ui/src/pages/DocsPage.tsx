import { useEffect, useState } from "react";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import Paper from "@mui/material/Paper";
import CircularProgress from "@mui/material/CircularProgress";
import Alert from "@mui/material/Alert";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Divider from "@mui/material/Divider";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import type { Components } from "react-markdown";
import { getDoc } from "../api/client";

function slugify(text: React.ReactNode): string {
  const str = extractText(text);
  return str
    .toLowerCase()
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .trim();
}

function extractText(node: React.ReactNode): string {
  if (typeof node === "string") return node;
  if (typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(extractText).join("");
  if (node && typeof node === "object" && "props" in node) {
    return extractText((node as React.ReactElement).props.children);
  }
  return "";
}

function DocLink({
  href,
  children
}: {
  href?: string;
  children?: React.ReactNode;
}) {
  const isAnchor = href?.startsWith("#");

  const handleAnchorClick = (e: React.MouseEvent<HTMLAnchorElement>) => {
    if (!isAnchor || !href) return;
    e.preventDefault();
    const targetId = href.slice(1);
    const el = document.getElementById(targetId);
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "start" });
      window.history.replaceState(null, "", href);
    }
  };

  if (isAnchor) {
    return (
      <a href={href} onClick={handleAnchorClick} style={{ color: "#90caf9" }}>
        {children}
      </a>
    );
  }

  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      style={{ color: "#90caf9" }}
    >
      {children}
    </a>
  );
}

const markdownComponents: Components = {
  h1: ({ children }) => (
    <Typography
      id={slugify(children)}
      variant="h4"
      sx={{ mt: 4, mb: 2, color: "primary.main" }}
    >
      {children}
    </Typography>
  ),
  h2: ({ children }) => (
    <>
      <Divider sx={{ mt: 4, mb: 1 }} />
      <Typography
        id={slugify(children)}
        variant="h5"
        sx={{ mt: 2, mb: 1.5, color: "primary.light" }}
      >
        {children}
      </Typography>
    </>
  ),
  h3: ({ children }) => (
    <Typography
      id={slugify(children)}
      variant="h6"
      sx={{ mt: 3, mb: 1, color: "secondary.light" }}
    >
      {children}
    </Typography>
  ),
  h4: ({ children }) => (
    <Typography
      id={slugify(children)}
      variant="subtitle1"
      sx={{ mt: 2.5, mb: 1, fontWeight: 700, color: "text.primary" }}
    >
      {children}
    </Typography>
  ),
  p: ({ children }) => (
    <Typography variant="body1" sx={{ mb: 1.5, lineHeight: 1.7 }}>
      {children}
    </Typography>
  ),
  a: ({ href, children }) => <DocLink href={href}>{children}</DocLink>,
  table: ({ children }) => (
    <Paper
      variant="outlined"
      sx={{ mb: 2, mt: 1, overflow: "auto", maxWidth: "100%" }}
    >
      <Table size="small">{children}</Table>
    </Paper>
  ),
  thead: ({ children }) => (
    <TableHead
      sx={{ "& th": { fontWeight: 700, bgcolor: "background.default" } }}
    >
      {children}
    </TableHead>
  ),
  tbody: ({ children }) => <TableBody>{children}</TableBody>,
  tr: ({ children }) => <TableRow>{children}</TableRow>,
  th: ({ children }) => (
    <TableCell sx={{ whiteSpace: "nowrap" }}>{children}</TableCell>
  ),
  td: ({ children }) => <TableCell>{children}</TableCell>,
  code: ({ className, children }) => {
    const isBlock = className?.startsWith("language-") || false;
    if (isBlock) {
      return (
        <Box
          component="pre"
          sx={{
            bgcolor: "background.default",
            border: 1,
            borderColor: "divider",
            borderRadius: 1,
            p: 2,
            mb: 2,
            overflow: "auto",
            fontFamily: '"JetBrains Mono", monospace',
            fontSize: "0.85rem",
            lineHeight: 1.5
          }}
        >
          <code>{children}</code>
        </Box>
      );
    }
    return (
      <Box
        component="code"
        sx={{
          bgcolor: "background.default",
          px: 0.75,
          py: 0.25,
          borderRadius: 0.5,
          fontFamily: '"JetBrains Mono", monospace',
          fontSize: "0.875em",
          color: "warning.light"
        }}
      >
        {children}
      </Box>
    );
  },
  pre: ({ children }) => <>{children}</>,
  ul: ({ children }) => (
    <Box component="ul" sx={{ mb: 1.5, pl: 3 }}>
      {children}
    </Box>
  ),
  ol: ({ children }) => (
    <Box component="ol" sx={{ mb: 1.5, pl: 3 }}>
      {children}
    </Box>
  ),
  li: ({ children }) => (
    <Typography
      component="li"
      variant="body1"
      sx={{ mb: 0.5, lineHeight: 1.7 }}
    >
      {children}
    </Typography>
  ),
  hr: () => <Divider sx={{ my: 3 }} />,
  blockquote: ({ children }) => (
    <Box
      sx={{
        borderLeft: 3,
        borderColor: "primary.main",
        pl: 2,
        py: 0.5,
        my: 2,
        bgcolor: "background.default",
        borderRadius: "0 4px 4px 0"
      }}
    >
      {children}
    </Box>
  ),
  strong: ({ children }) => (
    <Box component="strong" sx={{ fontWeight: 700, color: "text.primary" }}>
      {children}
    </Box>
  )
};

export default function DocsPage() {
  const [content, setContent] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    getDoc("state-inspector-guide")
      .then(md => {
        if (!cancelled) {
          setContent(md);
          setLoading(false);
        }
      })
      .catch(err => {
        if (!cancelled) {
          setError(err.message || "Failed to load documentation");
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  if (loading) {
    return (
      <Box sx={{ display: "flex", justifyContent: "center", mt: 8 }}>
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Alert severity="error" sx={{ mt: 2 }}>
        {error}
      </Alert>
    );
  }

  return (
    <Paper
      sx={{
        p: { xs: 2, md: 4 },
        maxWidth: 960,
        mx: "auto",
        mb: 4
      }}
    >
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={markdownComponents}
      >
        {content || ""}
      </ReactMarkdown>
    </Paper>
  );
}
