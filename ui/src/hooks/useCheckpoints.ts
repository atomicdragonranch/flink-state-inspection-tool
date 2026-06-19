import { useMutation } from "@tanstack/react-query";
import { discoverCheckpoints, discoverSavepoints } from "../api/client";

export function useDiscoverCheckpoints() {
  return useMutation({
    mutationFn: ({
      path,
      perJob,
      totalLimit
    }: {
      path: string;
      perJob?: number;
      totalLimit?: number;
    }) => discoverCheckpoints(path, perJob, totalLimit)
  });
}

export function useDiscoverSavepoints() {
  return useMutation({
    mutationFn: ({ path, totalLimit }: { path: string; totalLimit?: number }) =>
      discoverSavepoints(path, totalLimit)
  });
}
