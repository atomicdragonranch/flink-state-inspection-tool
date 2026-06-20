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
import AccountTreeIcon from "@mui/icons-material/AccountTree";

const navItems = [
  { label: "Browse", path: "/" },
  { label: "Inspect", path: "/inspect" },
  { label: "Diff", path: "/diff" },
  { label: "Cache", path: "/cache" }
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
        elevation={0}
        sx={{
          background: "linear-gradient(135deg, #0d2137 0%, #132f4c 50%, #0d2137 100%)",
          borderBottom: "1px solid",
          borderColor: "rgba(80, 144, 211, 0.2)"
        }}
      >
        <Toolbar>
          <Box
            component={Link}
            to="/"
            sx={{
              display: "flex",
              alignItems: "center",
              gap: 1.5,
              textDecoration: "none",
              mr: 4
            }}
          >
            <AccountTreeIcon
              sx={{
                color: "primary.main",
                fontSize: 28
              }}
            />
            <Box>
              <Typography
                variant="h6"
                sx={{
                  color: "#fff",
                  fontFamily: '"JetBrains Mono", monospace',
                  fontWeight: 700,
                  fontSize: "1.1rem",
                  lineHeight: 1.2,
                  letterSpacing: "-0.02em"
                }}
              >
                Flink State Inspector
              </Typography>
              <Typography
                variant="caption"
                sx={{
                  color: "rgba(255,255,255,0.45)",
                  fontSize: "0.65rem",
                  letterSpacing: "0.05em",
                  textTransform: "uppercase"
                }}
              >
                Checkpoint &amp; savepoint explorer
              </Typography>
            </Box>
          </Box>
          <Box sx={{ display: "flex", gap: 0.5 }}>
            {navItems.map(item => {
              const isActive = location.pathname === item.path;
              return (
                <Button
                  key={item.path}
                  component={Link}
                  to={item.path}
                  size="small"
                  aria-current={isActive ? "page" : undefined}
                  sx={{
                    color: isActive ? "#fff" : "rgba(255,255,255,0.6)",
                    bgcolor: isActive ? "rgba(80, 144, 211, 0.2)" : "transparent",
                    borderBottom: isActive ? "2px solid" : "2px solid transparent",
                    borderColor: isActive ? "primary.main" : "transparent",
                    borderRadius: "4px 4px 0 0",
                    px: 2,
                    fontWeight: isActive ? 600 : 400,
                    "&:hover": {
                      bgcolor: "rgba(80, 144, 211, 0.1)",
                      color: "#fff"
                    }
                  }}
                >
                  {item.label}
                </Button>
              );
            })}
          </Box>
          <Box sx={{ flexGrow: 1 }} />
          <Tooltip title="Documentation" arrow>
            <IconButton
              aria-label="Open documentation"
              onClick={() => window.open("/docs", "_blank", "noopener")}
              sx={{ color: "rgba(255,255,255,0.5)", "&:hover": { color: "#fff" } }}
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
