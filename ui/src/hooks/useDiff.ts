import { useMutation } from "@tanstack/react-query";
import { diffKeyed, diffBroadcast, type DiffRequest } from "../api/client";

export function useDiffKeyed() {
  return useMutation({
    mutationFn: (req: DiffRequest) => diffKeyed(req)
  });
}

export function useDiffBroadcast() {
  return useMutation({
    mutationFn: (req: DiffRequest) => diffBroadcast(req)
  });
}
