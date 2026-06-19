import { BrowserRouter, Routes, Route } from "react-router-dom";
import Layout from "./components/Layout";
import { AppStateProvider } from "./context/AppStateContext";
import BrowsePage from "./pages/BrowsePage";
import InspectPage from "./pages/InspectPage";
import DiffPage from "./pages/DiffPage";
import DocsPage from "./pages/DocsPage";

export default function App() {
  return (
    <BrowserRouter>
      <AppStateProvider>
        <Layout>
          <Routes>
            <Route path="/" element={<BrowsePage />} />
            <Route path="/inspect" element={<InspectPage />} />
            <Route path="/diff" element={<DiffPage />} />
            <Route path="/docs" element={<DocsPage />} />
          </Routes>
        </Layout>
      </AppStateProvider>
    </BrowserRouter>
  );
}
