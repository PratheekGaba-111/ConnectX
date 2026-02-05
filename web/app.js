const boardEl = document.getElementById('board');
const statusEl = document.getElementById('status');
const turnEl = document.getElementById('turn');
const timerEl = document.getElementById('timer');
const leaderboardEl = document.getElementById('leaderboard');
const joinButton = document.getElementById('join');
const usernameInput = document.getElementById('username');
const loginSection = document.getElementById('login');
const gameSection = document.getElementById('game');
const newGameButton = document.getElementById('new-game');
const resetButton = document.getElementById('reset');
const logoutButton = document.getElementById('logout');
const rematchPrompt = document.getElementById('rematch-prompt');
const rematchAcceptButton = document.getElementById('rematch-accept');
const rematchRejectButton = document.getElementById('rematch-reject');
const userDisplayEl = document.getElementById('user-display');
let socket;
let currentState;
let username = '';
let countdownInterval;
let isLoggedIn = false;
let shouldReconnect = true;

function buildBoard(board) {
  boardEl.innerHTML = '';
  board.forEach((row) => {
    row.forEach((cell, colIndex) => {
      const div = document.createElement('div');
      div.className = 'cell';
      if (cell === 1) div.classList.add('player1');
      if (cell === 2) div.classList.add('player2');
      div.addEventListener('click', () => sendMove(colIndex));
      boardEl.appendChild(div);
    });
  });
}

function updateState(state) {
  currentState = state;
  buildBoard(state.board);

  if (state.status === 'active') {
    rematchPrompt.classList.add('hidden');
  }

  if (state.status === 'finished') {
    statusEl.textContent = state.winner ? `Winner: ${state.winner}` : 'Game ended in a draw.';
  } else if (state.status === 'waiting') {
    statusEl.textContent = 'Waiting for opponent or bot...';
  } else {
    statusEl.textContent = state.message || '';
  }

  turnEl.textContent = state.turn ? `Turn: ${state.turn}` : '';
  updateTimer();
}

function updateTimer() {
  if (countdownInterval) clearInterval(countdownInterval);

  if (!currentState || currentState.status !== 'active' || !currentState.turnStartedAt) {
    timerEl.textContent = '';
    return;
  }

  const turnStarted = new Date(currentState.turnStartedAt).getTime();
  countdownInterval = setInterval(() => {
    const elapsed = Math.floor((Date.now() - turnStarted) / 1000);
    const remaining = Math.max(0, 30 - elapsed);
    timerEl.textContent = `Time left: ${remaining}s`;
    if (remaining === 0) clearInterval(countdownInterval);
  }, 500);
}

function sendMove(col) {
  if (!socket || socket.readyState !== WebSocket.OPEN) return;
  if (currentState && currentState.status === 'active' && currentState.turn === username) {
    socket.send(JSON.stringify({ type: 'move', column: col }));
  } else {
    statusEl.textContent = 'Please wait for your turn.';
  }
}

function sendReset() {
  if (socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({ type: 'reset' }));
  }
}

function sendRematchAccept() {
  if (socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({ type: 'rematch_accept' }));
  }
  rematchPrompt.classList.add('hidden');
}

function sendRematchReject() {
  if (socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({ type: 'rematch_reject' }));
  }
  rematchPrompt.classList.add('hidden');
}

function sendLogout() {
  if (socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({ type: 'logout' }));
  }
}

function connect() {
  if (socket?.readyState === WebSocket.OPEN) socket.close();

  shouldReconnect = true;
  socket = new WebSocket(`ws://${window.location.host}/ws`);

  socket.addEventListener('open', () => {
    socket.send(JSON.stringify({ type: 'join', username }));
    isLoggedIn = true;

    loginSection.classList.add('hidden');
    gameSection.classList.remove('hidden');
    userDisplayEl.textContent = `User: ${username}`;
    userDisplayEl.classList.remove('hidden');
  });

  socket.addEventListener('message', (event) => {
    const data = JSON.parse(event.data);

    if (data.type === 'state') {
      updateState(data);
      refreshLeaderboard();
    } else if (data.type === 'rematch_request') {
      rematchPrompt.classList.remove('hidden');
      const rematchText = document.getElementById('rematch-text');
      if (rematchText && data.message) {
        rematchText.textContent = data.message;
      }
      statusEl.textContent = '';
    } else if (data.type === 'status' || data.type === 'error') {
      statusEl.textContent = data.message;
    }
  });

  socket.addEventListener('close', () => {
    isLoggedIn = false;

    gameSection.classList.add('hidden');
    loginSection.classList.remove('hidden');
    userDisplayEl.classList.add('hidden');

    if (!shouldReconnect) {
      statusEl.textContent = 'Logged out.';
      return;
    }

    statusEl.textContent = 'Disconnected. Attempting to reconnect...';
    setTimeout(connect, 1000);
  });
}

function refreshLeaderboard() {
  fetch('/api/leaderboard')
    .then((res) => res.json())
    .then((leaders) => {
      leaderboardEl.innerHTML = '';
      leaders.forEach((leader) => {
        const li = document.createElement('li');
        li.textContent = `${leader.username}: ${leader.wins}`;
        leaderboardEl.appendChild(li);
      });
    })
    .catch(() => {});
}

function sendNewGame() {
  if (socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({ type: 'new_game' }));
  }
}

function startSession() {
  const requested = usernameInput.value.trim();
  if (requested) username = requested;

  if (!username) {
    alert('Username required');
    return;
  }

  connect();
}

joinButton.addEventListener('click', startSession);

logoutButton.addEventListener('click', () => {
  shouldReconnect = false;
  sendLogout();
  socket?.close();

  isLoggedIn = false;
  currentState = null;

  statusEl.textContent = 'Logged out.';
  loginSection.classList.remove('hidden');
  gameSection.classList.add('hidden');
  rematchPrompt.classList.add('hidden');
  userDisplayEl.textContent = '';
  userDisplayEl.classList.add('hidden');
});

newGameButton.addEventListener('click', () => {
  if (!isLoggedIn) return alert('Please login first.');
  sendNewGame();
});

resetButton.addEventListener('click', sendReset);
rematchAcceptButton.addEventListener('click', sendRematchAccept);
rematchRejectButton.addEventListener('click', sendRematchReject);
