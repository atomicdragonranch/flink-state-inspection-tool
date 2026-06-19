import { useMutation } from "@tanstack/react-query";
import { discoverOperators } from "../api/client";

export function useDiscoverOperators() {
  return useMutation({
    mutationFn: (path: string) => discoverOperators(path)
  });
}
