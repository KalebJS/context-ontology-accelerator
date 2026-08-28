// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import type { ReactNode } from "react";
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import SpaceBetween from "@cloudscape-design/components/space-between";
import { deleteOntologyCopy, ontologyTypeGroup } from "@utils/ontology-display";

/**
 * The warning shown inside every ontology delete-confirm modal.
 *
 * Single home for both halves of the copy. The header used to come from
 * `deleteOntologyCopy` while each page hand-wrote its own body, so the two
 * descriptions of the same destructive action could drift apart.
 */
export function OntologyDeleteWarning({
  ontologyType,
  name,
}: {
  ontologyType: string;
  /** Display name of the ontology, emphasised inline in the first sentence. */
  name: ReactNode;
}) {
  const { header } = deleteOntologyCopy(ontologyType);
  const group = ontologyTypeGroup(ontologyType);

  return (
    <Alert type="warning" header={header}>
      {group === "foundational" ? (
        <>
          Removing <b>{name}</b> deletes its graph triples and vector embeddings
          from this namespace, so it will no longer be available for grounding.
          It&apos;s a curated reference, so you can re-load it from the catalog
          afterward if needed.
        </>
      ) : group === "induced" ? (
        <SpaceBetween size="s">
          <Box variant="span">
            Deleting <b>{name}</b> permanently removes its classes, properties,
            graph triples, and vector embeddings from this namespace, so it can
            no longer ground future inductions and queries using its classes
            will stop returning results.
          </Box>
          <Box variant="span">
            It also deletes the proposals tied to it — both those that produced
            it and any grounded against it. This is generated content and cannot
            be recovered; re-creating it means re-running induction.
          </Box>
        </SpaceBetween>
      ) : (
        <>
          Deleting <b>{name}</b> permanently removes its graph triples, vector
          embeddings, and registry entry from this namespace. This is your own
          content and cannot be recovered.
        </>
      )}
    </Alert>
  );
}
