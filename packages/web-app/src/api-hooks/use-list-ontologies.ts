// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useQuery } from "@tanstack/react-query";
import {
  ListOntologiesCommand,
  type OntologyRecord,
} from "@coa/control-plane-client";
import { useControlPlaneClient } from "@components/ControlPlaneClientProvider";
import {
  hasOntologyId,
  type ListedOntology,
} from "../services/ontology-engine";

export type { OntologyRecord, ListedOntology };

export function useListOntologies(namespaceId: string | undefined) {
  const client = useControlPlaneClient();
  return useQuery<ListedOntology[], Error>({
    queryKey: ["ontologies", namespaceId],
    queryFn: async () => {
      const out = await client.send(
        new ListOntologiesCommand({ namespaceId: namespaceId! }),
      );
      // Same boundary narrowing as listOntologies(): the generated type makes
      // the @required id optional, so drop rows that can't be keyed or linked.
      return (out.ontologies ?? []).filter(hasOntologyId);
    },
    enabled: !!namespaceId,
  });
}
