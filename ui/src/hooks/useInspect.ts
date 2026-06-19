import { useMutation } from "@tanstack/react-query";
import {
  inspectKeyed,
  inspectBroadcast,
  type InspectRequest
} from "../api/client";

export function useInspectKeyed() {
  return useMutation({
    mutationFn: (req: InspectRequest) => inspectKeyed(req)
  });
}

export function useInspectBroadcast() {
  return useMutation({
    mutationFn: (req: InspectRequest) => inspectBroadcast(req)
  });
}
