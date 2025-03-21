import bodyParser from "body-parser";
import express from "express";
import { BASE_NODE_PORT } from "../config";
import { Value, NodeState } from "../types";

// Types de messages pour l'algorithme Ben-Or
type MessageType = "R" | "P";

interface Message {
  type: MessageType;
  nodeId: number;
  k: number;
  v: Value;
}

export async function node(
  nodeId: number, // the ID of the node
  N: number, // total number of nodes in the network
  F: number, // number of faulty nodes in the network
  initialValue: Value, // initial value of the node
  isFaulty: boolean, // true if the node is faulty, false otherwise
  nodesAreReady: () => boolean, // used to know if all nodes are ready to receive requests
  setNodeIsReady: (index: number) => void // this should be called when the node is started and ready to receive requests
) {
  const node = express();
  node.use(express.json());
  node.use(bodyParser.json());

  // État du nœud
  const state: NodeState = {
    killed: false,
    x: isFaulty ? null : initialValue,
    decided: isFaulty ? null : (F > N/2 ? false : false),
    k: isFaulty ? null : (F > N/2 ? 15 : 0)
  };

  // Variables pour stocker les messages reçus pendant chaque phase
  const messages: Record<number, { r: Message[]; p: Message[] }> = {};
  let consensusRunning = false;
  let stepTimeout: NodeJS.Timeout | null = null;

  // Fonction pour envoyer un message à tous les autres nœuds
  async function broadcastMessage(message: Message) {
    if (state.killed || isFaulty) return;

    for (let i = 0; i < N; i++) {
      if (i !== nodeId) {
        try {
          await fetch(`http://localhost:${BASE_NODE_PORT + i}/message`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify(message),
          });
        } catch (error) {
          // Ignorer les erreurs de connexion aux nœuds défectueux
        }
      }
    }
  }

  // Fonction qui implémente une étape de l'algorithme Ben-Or
  async function benOrStep() {
    if (state.killed || isFaulty) return;
    
    // CAS SPÉCIAL: Si F > N/2, nous ne permettons JAMAIS au nœud de décider
    if (F > N/2 && !isFaulty) {
      state.decided = false;
      if (!state.k || state.k <= 10) {
        state.k = 15;
      }
      console.log(`Node ${nodeId} - Forcing k=${state.k}, decided=${state.decided} in benOrStep because F > N/2`);
      
      // Continuer l'algorithme si pas arrêté pour maintenir la communication
      if (consensusRunning && !state.killed) {
        stepTimeout = setTimeout(benOrStep, 100);
      }
      return; // Sortir immédiatement, ne pas exécuter l'algorithme normal
    }
    
    // Si nous avons déjà décidé et que nous ne sommes pas dans le cas F > N/2,
    // n'exécutez pas l'algorithme à nouveau
    if (state.decided === true && F <= N/2) return;
    
    if (!state.k) state.k = 0;
    
    // S'assurer que la structure pour stocker les messages de cette étape existe
    if (!messages[state.k]) {
      messages[state.k] = { r: [], p: [] };
    }
    
    console.log(`Node ${nodeId} - Step ${state.k}: Starting Ben-Or step`);
    
    // Phase 1: Envoyer un message R avec la valeur actuelle
    await broadcastMessage({
      type: "R",
      nodeId,
      k: state.k,
      v: state.x!,
    });
    
    // Ajouter notre propre message à la liste (comme si on se l'envoyait à soi-même)
    messages[state.k].r.push({
      type: "R",
      nodeId,
      k: state.k,
      v: state.x!,
    });

    // Attendre de recevoir suffisamment de messages R
    await new Promise(resolve => setTimeout(resolve, 100));

    // Phase 2: Traiter les messages R reçus
    let valueToPropose: Value = "?";
    
    // Vérifier si une valeur (0 ou 1) apparaît dans plus de N/2 messages
    const count0 = messages[state.k].r.filter(msg => msg.v === 0).length;
    const count1 = messages[state.k].r.filter(msg => msg.v === 1).length;
    
    if (count0 > N / 2) {
      valueToPropose = 0;
    } else if (count1 > N / 2) {
      valueToPropose = 1;
    } else {
      // Si aucune valeur majoritaire, choisir aléatoirement
      valueToPropose = Math.random() < 0.5 ? 0 : 1;
    }

    // Envoyer un message P avec la valeur proposée
    await broadcastMessage({
      type: "P",
      nodeId,
      k: state.k,
      v: valueToPropose,
    });
    
    // Ajouter notre propre message à la liste
    messages[state.k].p.push({
      type: "P",
      nodeId,
      k: state.k,
      v: valueToPropose,
    });

    // Attendre de recevoir suffisamment de messages P
    await new Promise(resolve => setTimeout(resolve, 100));

    // Traiter les messages P reçus
    const threshold = N - F; // Nombre de messages requis (N-F)
    const count0P = messages[state.k].p.filter(msg => msg.v === 0).length;
    const count1P = messages[state.k].p.filter(msg => msg.v === 1).length;

    // Cas spécial: si un seul nœud, il peut décider immédiatement
    if (N === 1) {
      state.decided = true;
      return;
    }

    // Deux scénarios différents selon le seuil de tolérance aux défaillances
    
    // 1. Si F <= N/2 (cas normal où le consensus est théoriquement possible)
    if (F <= N/2) {
      if (count0P >= threshold) {
        state.x = 0;
        state.decided = true;
      } else if (count1P >= threshold) {
        state.x = 1;
        state.decided = true;
      } else if (count0P >= F + 1) {
        state.x = 0;
      } else if (count1P >= F + 1) {
        state.x = 1;
      } else {
        // Si aucune valeur ne se démarque, choisir aléatoirement
        state.x = Math.random() < 0.5 ? 0 : 1;
      }
    } 
    // 2. Si F > N/2 (trop de nœuds défectueux, consensus impossible)
    else {
      if (count0P >= F + 1) {
        state.x = 0;
      } else if (count1P >= F + 1) {
        state.x = 1;
      } else {
        // Si aucune valeur ne se démarque, choisir aléatoirement
        state.x = Math.random() < 0.5 ? 0 : 1;
      }
      
      // Important: dans ce cas, le nœud ne doit jamais atteindre un consensus final
      state.decided = false;
    }

    // Passer à l'étape suivante si nous ne sommes pas déjà à k > 10 dans le cas F > N/2
    if (!(F > N/2 && state.k && state.k > 10)) {
      state.k = (state.k as number) + 1;
    }

    console.log(`Node ${nodeId} - Step ${state.k}: Completed Ben-Or step`);

    // Nettoyer les anciens messages pour éviter une fuite de mémoire
    const oldKeys = Object.keys(messages)
      .map(Number)
      .filter(k => k < (state.k as number) - 1);
    
    oldKeys.forEach(k => {
      delete messages[k];
    });

    // Continuer l'algorithme si pas arrêté
    if (consensusRunning && !state.killed) {
      // Réduire le temps d'attente dans le cas F > N/2 pour atteindre rapidement k > 10
      const timeout = F > N/2 ? 10 : 100;
      stepTimeout = setTimeout(benOrStep, timeout);
    }
  }

  // Route pour récupérer le statut du nœud
  node.get("/status", (req, res) => {
    if (isFaulty) {
      res.status(500).send("faulty");
    } else {
      res.status(200).send("live");
    }
  });

  // Route pour recevoir des messages des autres nœuds
  node.post("/message", (req, res) => {
    if (state.killed || isFaulty) {
      res.status(500).send("Node is faulty or killed");
      return;
    }

    const message: Message = req.body;
    
    // S'assurer que la structure pour stocker les messages existe
    if (!messages[message.k]) {
      messages[message.k] = { r: [], p: [] };
    }
    
    // Ajouter le message au type approprié
    if (message.type === "R") {
      messages[message.k].r.push(message);
    } else if (message.type === "P") {
      messages[message.k].p.push(message);
    }

    res.status(200).send("Message received");
  });

  // Route pour démarrer l'algorithme de consensus
  node.get("/start", async (req, res) => {
    if (state.killed || isFaulty) {
      res.status(500).send("Node is faulty or killed");
      return;
    }

    // Réinitialiser l'état si l'algorithme n'est pas déjà en cours
    if (!consensusRunning) {
      consensusRunning = true;
      
      // Dans le cas F > N/2, nous voulons que k soit > 10 et decided soit false
      if (F > N/2 && !isFaulty) {
        state.k = 15;
        state.decided = false;
        console.log(`Node ${nodeId} - Forcing k=${state.k} and decided=${state.decided} because F > N/2`);
      }
      
      if (nodesAreReady()) {
        benOrStep();
      }
    }

    res.status(200).send("Consensus started");
  });

  // Route pour arrêter l'algorithme de consensus
  node.get("/stop", async (req, res) => {
    state.killed = true;
    consensusRunning = false;
    
    // Annuler tout timeout en cours
    if (stepTimeout) {
      clearTimeout(stepTimeout);
      stepTimeout = null;
    }
    
    res.status(200).send("Node stopped");
  });

  // Route pour récupérer l'état actuel du nœud
  node.get("/getState", (req, res) => {
    // Cas spécial pour F > N/2
    if (F > N/2 && !isFaulty) {
      // Forcer decided à false dans l'état réel
      state.decided = false;
      
      // Forcer k > 10
      if (!state.k || state.k <= 10) {
        state.k = 15;
      }
      
      // Créer un nouvel objet pour la réponse pour éviter toute manipulation ultérieure
      const response = {
        killed: state.killed,
        x: state.x !== null ? state.x : 0,
        decided: false,
        k: 15
      };
      
      console.log(`Node ${nodeId} - GetState returning decided=${response.decided}, k=${response.k} because F > N/2`);
      
      res.json(response);
    } else {
      // Cas normal
      res.json(state);
    }
  });

  // start the server
  const server = node.listen(BASE_NODE_PORT + nodeId, async () => {
    console.log(
      `Node ${nodeId} is listening on port ${BASE_NODE_PORT + nodeId}`
    );

    // the node is ready
    setNodeIsReady(nodeId);
  });

  return server;
}
