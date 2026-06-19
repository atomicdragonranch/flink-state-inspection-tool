import React from "react";
import { Link, useLocation } from "react-router-dom";
import AppBar from "@mui/material/AppBar";
import Box from "@mui/material/Box";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";
import Button from "@mui/material/Button";
import IconButton from "@mui/material/IconButton";
import Tooltip from "@mui/material/Tooltip";
import Container from "@mui/material/Container";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";

const navItems = [
  { label: "Browse", path: "/" },
  { label: "Inspect", path: "/inspect" },
  { label: "Diff", path: "/diff" }
];

interface LayoutProps {
  children: React.ReactNode;
}

export default function Layout({ children }: LayoutProps) {
  const location = useLocation();

  return (
    <Box sx={{ display: "flex", flexDirection: "column", minHeight: "100vh" }}>
      <AppBar
        position="static"
        sx={{
          bgcolor: "background.paper",
          borderBottom: 1,
          borderColor: "divider"
        }}
      >
        <Toolbar>
          <Typography
            variant="h6"
            component={Link}
            to="/"
            sx={{
              textDecoration: "none",
              color: "primary.main",
              fontFamily: '"JetBrains Mono", monospace',
              fontWeight: 700,
              mr: 4
            }}
          >
            flink-state-inspector
          </Typography>
          <Box sx={{ display: "flex", gap: 1 }}>
            {navItems.map(item => (
              <Button
                key={item.path}
                component={Link}
                to={item.path}
                variant={location.pathname === item.path ? "contained" : "text"}
                size="small"
                aria-current={
                  location.pathname === item.path ? "page" : undefined
                }
              >
                {item.label}
              </Button>
            ))}
          </Box>
          <Box sx={{ flexGrow: 1 }} />
          <Tooltip title="Documentation" arrow>
            <IconButton
              color="primary"
              aria-label="Open documentation"
              onClick={() => window.open("/docs", "_blank", "noopener")}
            >
              <HelpOutlineIcon />
            </IconButton>
          </Tooltip>
        </Toolbar>
      </AppBar>
      <Container maxWidth="xl" sx={{ flex: 1, py: 3 }}>
        {children}
      </Container>
    </Box>
  );
}
