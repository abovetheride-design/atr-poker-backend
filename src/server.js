import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import http from 'node:http';
import { randomInt } from 'node:crypto';
import { Server } from 'socket.io';
import { createClient } from '@supabase/supabase-js';

const PORT = Number(process.env.PORT || 3001);
const origins = String(process.env.CLIENT_ORIGIN || '')
  .split(',').map(s => s.trim()).filter(Boolean);

for (const key of ['SUPABASE_URL','SUPABASE_PUBLISHABLE_KEY','SUPABASE_SERVICE_ROLE_KEY']) {
  if (!process.env[key]) throw new Error(`Missing ${key}`);
}

const authClient = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_PUBLISHABLE_KEY,
  { auth: { persistSession: false, autoRefreshToken: false } }
);

const db = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  { auth: { persistSession: false, autoRefreshToken: false } }
);

const app = express();
app.use(cors({ origin: origins.length ? origins : true, credentials: true }));
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: origins.length ? origins : true, credentials: true },
  transports: ['websocket', 'polling']
});

const TABLE_ROOM = id => `poker:${id}`;
const userSockets = new Map();

// V13.3 authoritative hand state lives on the server.
// Private hole cards are NEVER included in room-wide broadcasts.
const liveHands = new Map();
const dealerByTable = new Map();
const handNoByTable = new Map();
const startTimers = new Map();
const turnTimers = new Map();
const leaveAfterHandUsers = new Set();
const TURN_MS = 20000;


function makeDeck() {
  const suits = ['♠','♥','♦','♣'];
  const ranks = ['2','3','4','5','6','7','8','9','10','J','Q','K','A'];
  const deck = [];
  for (const s of suits) {
    for (let i = 0; i < ranks.length; i++) {
      deck.push({ r: ranks[i], s, v: i + 2 });
    }
  }
  for (let i = deck.length - 1; i > 0; i--) {
    const j = randomInt(i + 1);
    [deck[i], deck[j]] = [deck[j], deck[i]];
  }
  return deck;
}

function nextOccupiedSeat(seatNos, from) {
  if (!seatNos.length) return null;
  const sorted = [...seatNos].sort((a,b)=>a-b);
  for (const seat of sorted) if (seat > from) return seat;
  return sorted[0];
}

function handPublicState(hand) {
  const completed = !!hand.completed;
  return {
    tableId: hand.tableId,
    handNo: hand.handNo,
    handActive: !completed,
    street: completed ? 'complete' : hand.street,
    showdown: !!hand.finalShowdown,
    board: hand.board,
    pot: completed ? 0 : hand.pot,
    potWon: completed ? Number(hand.finalPotWon || 0) : 0,
    currentBet: completed ? 0 : hand.currentBet,
    dealerSeatNo: hand.dealerSeatNo,
    sbSeatNo: hand.sbSeatNo,
    bbSeatNo: hand.bbSeatNo,
    turnSeatNo: completed ? null : hand.turnSeatNo,
    turnDeadline: completed ? null : (hand.turnDeadline || null),
    lastAction: hand.lastAction || '',
    winnerUserIds: completed ? (hand.finalWinnerUserIds || []) : [],
    revealedHoles: completed ? (hand.finalRevealedHoles || {}) : {},
    resultText: completed ? (hand.finalResultText || 'HAND COMPLETE') : '',
    sidePotResults: completed ? (hand.finalSidePotResults || []) : [],
    resultHoldMs: completed ? 3500 : 0,
    sidePots: buildSidePots(hand).map(p=>({amount:p.amount,eligibleUserIds:p.eligibleUserIds})),
    players: hand.players.map(p => ({
      seatNo: p.seatNo,
      userId: p.userId,
      stack: p.stack,
      streetBet: completed ? 0 : p.streetBet,
      totalContrib: Number(p.totalContrib||0),
      folded: p.folded,
      allIn: p.allIn
    }))
  };
}

function emitPrivateHoleCards(hand) {
  for (const p of hand.players) {
    const socketId = userSockets.get(p.userId);
    if (!socketId) continue;
    io.to(socketId).emit('poker:hand:private', {
      tableId: hand.tableId,
      handNo: hand.handNo,
      hole: hand.holeByUser.get(p.userId) || []
    });
  }
}


function clearTurnTimer(tableId){
  const timer=turnTimers.get(tableId);
  if(timer)clearTimeout(timer);
  turnTimers.delete(tableId);
}

function armTurnTimer(hand){
  clearTurnTimer(hand.tableId);
  if(hand.completed || hand.turnSeatNo===null || hand.turnSeatNo===undefined){
    hand.turnDeadline=null;
    return;
  }

  hand.turnDeadline=Date.now()+TURN_MS;

  const expectedHandNo=hand.handNo;
  const expectedSeat=hand.turnSeatNo;

  const timer=setTimeout(async()=>{
    try{
      const current=liveHands.get(hand.tableId);
      if(!current || current.completed)return;
      if(current.handNo!==expectedHandNo || current.turnSeatNo!==expectedSeat)return;

      const p=bySeat(current,expectedSeat);
      if(!p || p.folded || p.allIn)return;

      const need=Math.max(0,current.currentBet-p.streetBet);
      current.lastAction=need>0
        ? `SEAT ${p.seatNo} AUTO-FOLD (TIME)`
        : `SEAT ${p.seatNo} AUTO-CHECK (TIME)`;

      const result=await doServerAction(
        current,
        p.userId,
        {type:need>0?'fold':'call'},
        {fromTimer:true}
      );

      if(!result?.ok){
        console.error('ATR Poker timeout action rejected:',result);
        return;
      }

      if(result?.state && !result.state.handActive){
        return;
      }
    }catch(error){
      console.error('ATR Poker turn timeout:',error);
    }
  },TURN_MS);

  turnTimers.set(hand.tableId,timer);
}

function emitHandState(hand) {
  io.to(TABLE_ROOM(hand.tableId)).emit('poker:hand:state', handPublicState(hand));
  emitPrivateHoleCards(hand);
}

async function persistHandStacks(hand) {
  for (const p of hand.players) {
    const { error } = await db.from('poker_seats')
      .update({ stack: p.stack })
      .eq('table_id', hand.tableId)
      .eq('seat_no', p.seatNo)
      .eq('user_id', p.userId);
    if (error) throw error;
  }
}

async function abortLiveHand(tableId, { restoreStacks = true } = {}) {
  clearTurnTimer(tableId);
  const hand = liveHands.get(tableId);
  if (!hand) return;

  if (restoreStacks) {
    for (const p of hand.players) {
      const restored = hand.initialStacks.get(p.userId);
      if (restored === undefined) continue;
      await db.from('poker_seats')
        .update({ stack: restored })
        .eq('table_id', tableId)
        .eq('user_id', p.userId);
    }
  }

  liveHands.delete(tableId);
  io.to(TABLE_ROOM(tableId)).emit('poker:hand:state', {
    tableId,
    handNo: hand.handNo,
    handActive: false,
    street: 'idle',
    board: [],
    pot: 0,
    currentBet: 0,
    dealerSeatNo: null,
    sbSeatNo: null,
    bbSeatNo: null,
    turnSeatNo: null,
    players: []
  });
}

async function startServerHand(tableId) {
  if (liveHands.has(tableId)) return false;

  const snapshot = await tableSnapshot(tableId);
  const table = snapshot.table;
  const eligible = snapshot.seats
    .filter(s => Number(s.stack) > 0)
    .sort((a,b)=>Number(a.seat_no)-Number(b.seat_no));

  if (eligible.length < 2) return false;

  const seatNos = eligible.map(s => Number(s.seat_no));
  const previousDealer = dealerByTable.get(tableId);
  const dealerSeatNo = previousDealer === undefined
    ? seatNos[0]
    : nextOccupiedSeat(seatNos, previousDealer);
  dealerByTable.set(tableId, dealerSeatNo);

  let sbSeatNo, bbSeatNo, turnSeatNo;
  if (eligible.length === 2) {
    // Heads-up Hold'em: dealer posts SB and acts first preflop.
    sbSeatNo = dealerSeatNo;
    bbSeatNo = nextOccupiedSeat(seatNos, dealerSeatNo);
    turnSeatNo = dealerSeatNo;
  } else {
    sbSeatNo = nextOccupiedSeat(seatNos, dealerSeatNo);
    bbSeatNo = nextOccupiedSeat(seatNos, sbSeatNo);
    turnSeatNo = nextOccupiedSeat(seatNos, bbSeatNo);
  }

  const deck = makeDeck();
  const holeByUser = new Map();
  for (const seat of eligible) {
    holeByUser.set(seat.user_id, [deck.pop(), deck.pop()]);
  }

  const players = eligible.map(s => ({
    seatNo: Number(s.seat_no),
    userId: s.user_id,
    stack: Number(s.stack),
    streetBet: 0,
    totalContrib: 0,
    folded: false,
    allIn: false
  }));

  const initialStacks = new Map(players.map(p => [p.userId, p.stack]));
  let pot = 0;

  function postBlind(seatNo, amount) {
    const p = players.find(x => x.seatNo === seatNo);
    if (!p) return;
    const paid = Math.min(Number(amount), p.stack);
    p.stack -= paid;
    p.streetBet += paid;
    p.totalContrib += paid;
    p.allIn = p.stack === 0;
    pot += paid;
  }

  postBlind(sbSeatNo, table.sb);
  postBlind(bbSeatNo, table.bb);

  const handNo = (handNoByTable.get(tableId) || 0) + 1;
  handNoByTable.set(tableId, handNo);

  const hand = {
    tableId,
    handNo,
    street: 'preflop',
    board: [],
    deck,
    pot,
    currentBet: Math.max(Number(table.sb), Number(table.bb)),
    bigBlind:Number(table.bb),
    lastFullRaise:Number(table.bb),
    dealerSeatNo,
    sbSeatNo,
    bbSeatNo,
    turnSeatNo,
    players,
    holeByUser,
    initialStacks,
    actedThisStreet: new Set(),
    lastAction: 'BLINDS POSTED'
  };

  await persistHandStacks(hand);
  liveHands.set(tableId, hand);
  armTurnTimer(hand);
  emitHandState(hand);
  return true;
}


function livePlayers(hand){return hand.players.filter(p=>!p.folded);}
function canActPlayers(hand){return hand.players.filter(p=>!p.folded&&!p.allIn);}
function bySeat(hand,s){return hand.players.find(p=>p.seatNo===s);}
function nextCanAct(hand,from){return nextOccupiedSeat(canActPlayers(hand).map(p=>p.seatNo),from);}
function roundDone(hand){
  const actors=canActPlayers(hand);
  // Never close the betting round while a player who can still act owes a call.
  if(actors.some(p=>p.streetBet<hand.currentBet))return false;
  if(actors.length<=1)return true;
  return actors.every(p=>hand.actedThisStreet.has(p.userId));
}
function nextStreet(s){return s==='preflop'?'flop':s==='flop'?'turn':s==='turn'?'river':'showdown';}
function dealStreet(hand,s){
  if(s==='flop')hand.board.push(hand.deck.pop(),hand.deck.pop(),hand.deck.pop());
  else if(s==='turn'||s==='river')hand.board.push(hand.deck.pop());
}

function buildSidePots(hand){
  const contributors=hand.players.filter(p=>Number(p.totalContrib||0)>0);
  const levels=[...new Set(contributors.map(p=>Number(p.totalContrib||0)))].sort((a,b)=>a-b);
  const pots=[];
  let previous=0;

  for(const level of levels){
    const involved=contributors.filter(p=>Number(p.totalContrib||0)>=level);
    const amount=(level-previous)*involved.length;
    if(amount>0){
      pots.push({
        amount,
        cap:level,
        contributorUserIds:involved.map(p=>p.userId),
        eligibleUserIds:involved.filter(p=>!p.folded).map(p=>p.userId)
      });
    }
    previous=level;
  }
  return pots;
}

function splitAmount(amount,winnerIds){
  const payouts=new Map();
  if(!winnerIds.length)return payouts;
  const share=Math.floor(amount/winnerIds.length);
  let remainder=amount-share*winnerIds.length;
  for(const id of winnerIds){
    payouts.set(id,share+(remainder>0?1:0));
    if(remainder>0)remainder--;
  }
  return payouts;
}

function mergePayouts(target,source){
  for(const [id,amount] of source){
    target.set(id,(target.get(id)||0)+amount);
  }
}

function showdownPayouts(hand){
  const pots=buildSidePots(hand);
  const payouts=new Map();
  const summaries=[];

  for(const pot of pots){
    const eligible=hand.players.filter(p=>pot.eligibleUserIds.includes(p.userId));
    if(!eligible.length)continue;

    let best=null,winners=[];
    for(const p of eligible){
      const r=best7([...(hand.holeByUser.get(p.userId)||[]),...hand.board]);
      if(!best||cmp(r,best)>0){best=r;winners=[p.userId];}
      else if(cmp(r,best)===0)winners.push(p.userId);
    }

    mergePayouts(payouts,splitAmount(pot.amount,winners));
    summaries.push({
      amount:pot.amount,
      winnerUserIds:winners,
      label:rankNames[best?.[0]||0]
    });
  }

  return {payouts,summaries};
}

function rank5(cs){
  const v=cs.map(c=>c.v).sort((a,b)=>b-a), m=new Map(); v.forEach(x=>m.set(x,(m.get(x)||0)+1));
  const g=[...m].sort((a,b)=>b[1]-a[1]||b[0]-a[0]), flush=cs.every(c=>c.s===cs[0].s);
  const u=[...new Set(v)]; if(u[0]===14)u.push(1); let sh=0;
  for(let i=0;i<=u.length-5;i++)if(u[i]-u[i+4]===4){sh=u[i];break;}
  if(flush&&sh)return[8,sh]; if(g[0][1]===4)return[7,g[0][0],g[1][0]];
  if(g[0][1]===3&&g[1]?.[1]>=2)return[6,g[0][0],g[1][0]]; if(flush)return[5,...v];
  if(sh)return[4,sh]; if(g[0][1]===3)return[3,g[0][0],...g.slice(1).map(x=>x[0]).sort((a,b)=>b-a)];
  if(g[0][1]===2&&g[1]?.[1]===2){const hi=Math.max(g[0][0],g[1][0]),lo=Math.min(g[0][0],g[1][0]);return[2,hi,lo,g.find(x=>x[1]===1)?.[0]||0];}
  if(g[0][1]===2)return[1,g[0][0],...g.slice(1).map(x=>x[0]).sort((a,b)=>b-a)]; return[0,...v];
}
function cmp(a,b){for(let i=0;i<Math.max(a.length,b.length);i++){const d=(a[i]||0)-(b[i]||0);if(d)return d;}return 0;}
function best7(cs){let z=null;for(let a=0;a<3;a++)for(let b=a+1;b<4;b++)for(let c=b+1;c<5;c++)for(let d=c+1;d<6;d++)for(let e=d+1;e<7;e++){const r=rank5([cs[a],cs[b],cs[c],cs[d],cs[e]]);if(!z||cmp(r,z)>0)z=r;}return z;}
const rankNames=['HIGH CARD','PAIR','TWO PAIR','THREE OF A KIND','STRAIGHT','FLUSH','FULL HOUSE','FOUR OF A KIND','STRAIGHT FLUSH'];

async function finishServerHand(hand,winnerIds,label,{showdown=false}={}){
  const potWon=hand.pot;
  const share=Math.floor(potWon/winnerIds.length);
  let rem=potWon-share*winnerIds.length;

  for(const id of winnerIds){
    const p=hand.players.find(x=>x.userId===id);
    if(p){
      p.stack+=share+(rem>0?1:0);
      if(rem>0)rem--;
    }
  }

  await persistHandStacks(hand);

  // Keep the finished hand registered during the result screen.
  // This removes the timing window where a client still shows River but the server
  // has already deleted the hand and replies NO_ACTIVE_HAND.
  clearTurnTimer(hand.tableId);
  hand.completed=true;
  hand.handActive=false;
  hand.turnSeatNo=null;
  hand.turnDeadline=null;

  const text=winnerIds.length===1
    ? `${label} · POT ${potWon} WON`
    : `${label} · POT ${potWon} SPLIT`;

  const revealedHoles={};
  if(showdown){
    for(const p of livePlayers(hand)){
      revealedHoles[p.userId]=hand.holeByUser.get(p.userId)||[];
    }
  }

  hand.finalShowdown=showdown;
  hand.finalPotWon=potWon;
  hand.finalWinnerUserIds=[...winnerIds];
  hand.finalRevealedHoles=revealedHoles;
  hand.finalResultText=text;

  const finalState=handPublicState(hand);
  io.to(TABLE_ROOM(hand.tableId)).emit('poker:hand:state',finalState);

  /* legacy object removed in V13.4.4 */
  /*
  io.to(TABLE_ROOM(hand.tableId)).emit('poker:hand:state',{
    tableId:hand.tableId,
    handNo:hand.handNo,
    handActive:false,
    street:'complete',
    showdown,
    board:hand.board,
    pot:0,
    potWon,
    currentBet:0,
    dealerSeatNo:hand.dealerSeatNo,
    sbSeatNo:hand.sbSeatNo,
    bbSeatNo:hand.bbSeatNo,
    turnSeatNo:null,
    players:hand.players.map(p=>({
      seatNo:p.seatNo,
      userId:p.userId,
      stack:p.stack,
      streetBet:0,
      folded:p.folded,
      allIn:p.allIn
    })),
    winnerUserIds,
    revealedHoles,
    resultText:text,
    resultHoldMs:3500
  });
  */

  io.emit('poker:lobby:changed');

  // Keep the completed hand on-screen long enough to read the winner.
  setTimeout(async()=>{
    try{
      const current=liveHands.get(hand.tableId);
      if(current===hand)liveHands.delete(hand.tableId);

      await processLeaveAfterHand(hand.tableId);
      const fresh=await tableSnapshot(hand.tableId);
      io.to(TABLE_ROOM(hand.tableId)).emit('poker:table:state',fresh);
      scheduleServerHand(hand.tableId,400);
    }catch(error){
      console.error('ATR Poker result hold:',error);
    }
  },3500);

  return finalState;
}

async function finishServerHandWithPayouts(hand,payouts,winnerIds,summaries){
  const potWon=hand.pot;

  for(const [id,amount] of payouts){
    const p=hand.players.find(x=>x.userId===id);
    if(p)p.stack+=amount;
  }

  await persistHandStacks(hand);
  clearTurnTimer(hand.tableId);
  hand.completed=true;
  hand.handActive=false;
  hand.turnSeatNo=null;
  hand.turnDeadline=null;

  const revealedHoles={};
  for(const p of livePlayers(hand)){
    revealedHoles[p.userId]=hand.holeByUser.get(p.userId)||[];
  }

  const label=summaries.length>1
    ? `${summaries.length} POTS SETTLED`
    : (summaries[0]?.label||'SHOWDOWN');
  const text=winnerIds.length===1
    ? `${label} · POT ${potWon} WON`
    : `${label} · POT ${potWon} SPLIT`;

  hand.finalShowdown=true;
  hand.finalPotWon=potWon;
  hand.finalWinnerUserIds=[...winnerIds];
  hand.finalRevealedHoles=revealedHoles;
  hand.finalResultText=text;
  hand.finalSidePotResults=summaries;

  const finalState=handPublicState(hand);
  finalState.sidePotResults=summaries;
  io.to(TABLE_ROOM(hand.tableId)).emit('poker:hand:state',finalState);
  io.emit('poker:lobby:changed');

  setTimeout(async()=>{
    try{
      const current=liveHands.get(hand.tableId);
      if(current===hand)liveHands.delete(hand.tableId);

      await processLeaveAfterHand(hand.tableId);
      const fresh=await tableSnapshot(hand.tableId);
      io.to(TABLE_ROOM(hand.tableId)).emit('poker:table:state',fresh);
      scheduleServerHand(hand.tableId,400);
    }catch(error){
      console.error('ATR Poker side-pot result hold:',error);
    }
  },3500);

  return finalState;
}

async function serverShowdown(hand){
  while(hand.board.length<5)hand.board.push(hand.deck.pop());

  const {payouts,summaries}=showdownPayouts(hand);
  const winnerIds=[...payouts.keys()];

  return await finishServerHandWithPayouts(
    hand,
    payouts,
    winnerIds,
    summaries
  );
}
async function advanceServerStreet(hand){
  if(livePlayers(hand).length===1)return finishServerHand(hand,[livePlayers(hand)[0].userId],'FOLD');
  if(hand.street==='river')return serverShowdown(hand);
  const s=nextStreet(hand.street); dealStreet(hand,s); hand.street=s; hand.currentBet=0; hand.players.forEach(p=>p.streetBet=0); hand.actedThisStreet=new Set();
  const actors=canActPlayers(hand);
  if(actors.length===0)return serverShowdown(hand);
  if(actors.length===1){
    const lone=actors[0];
    if(lone.streetBet===hand.currentBet)return serverShowdown(hand);
  }
  hand.turnSeatNo=nextOccupiedSeat(actors.map(p=>p.seatNo),hand.dealerSeatNo);
  hand.lastAction=s.toUpperCase();
  armTurnTimer(hand);
  emitHandState(hand);
}
async function doServerAction(hand,userId,payload,{fromTimer=false}={}){
  clearTurnTimer(hand.tableId);
  hand.turnDeadline=null;
  const p=hand.players.find(x=>x.userId===userId); if(!p)return{ok:false,error:'NOT_IN_HAND'};
  if(p.seatNo!==hand.turnSeatNo)return{ok:false,error:'NOT_YOUR_TURN'};
  const type=String(payload.type||'').toLowerCase(), need=Math.max(0,hand.currentBet-p.streetBet);
  if(type==='fold'){
    p.folded=true;hand.actedThisStreet.add(userId);
    if(!fromTimer)hand.lastAction=`SEAT ${p.seatNo} FOLD`;
  }
  else if(type==='call'){
    const paid=Math.min(need,p.stack);p.stack-=paid;p.streetBet+=paid;p.totalContrib+=paid;hand.pot+=paid;p.allIn=p.stack===0;hand.actedThisStreet.add(userId);
    if(!fromTimer)hand.lastAction=need?`SEAT ${p.seatNo} CALL ${paid}`:`SEAT ${p.seatNo} CHECK`;
  }
  else if(type==='raise'){
    const target=Math.floor(Number(payload.amount||0));
    const maxTarget=p.streetBet+p.stack;
    if(!Number.isFinite(target)||target<=hand.currentBet)return{ok:false,error:'RAISE_TOO_SMALL'};
    if(target>maxTarget)return{ok:false,error:'INVALID_RAISE'};

    const raiseSize=target-hand.currentBet;
    const minRaise=Number(hand.lastFullRaise||hand.bigBlind||20);
    const isAllIn=target===maxTarget;

    // A short raise is legal only when it is a genuine all-in.
    if(raiseSize<minRaise && !isAllIn){
      return{ok:false,error:`MIN_RAISE_${hand.currentBet+minRaise}`};
    }

    const add=target-p.streetBet;
    p.stack-=add;
    p.streetBet=target;
    p.totalContrib+=add;
    hand.pot+=add;
    p.allIn=p.stack===0;

    if(raiseSize>=minRaise){
      hand.lastFullRaise=raiseSize;
      hand.actedThisStreet=new Set([userId]);
    }else{
      // Short all-in does not reopen betting for players who already acted.
      hand.actedThisStreet.add(userId);
    }

    hand.currentBet=Math.max(hand.currentBet,target);
    hand.lastAction=`SEAT ${p.seatNo} ${p.allIn?'ALL-IN':'RAISE'} ${target}`;
  }else return{ok:false,error:'BAD_ACTION'};
  await persistHandStacks(hand);

  if(livePlayers(hand).length===1){
    const finalState=await finishServerHand(hand,[livePlayers(hand)[0].userId],'FOLD');
    return{ok:true,state:finalState};
  }

  if(roundDone(hand)){
    const state=await advanceServerStreet(hand);
    return{ok:true,state:state || handPublicState(hand)};
  }

  const nextSeat=nextCanAct(hand,p.seatNo);
  if(nextSeat===null||nextSeat===undefined){
    return{ok:false,error:'NO_NEXT_ACTOR'};
  }

  hand.turnSeatNo=nextSeat;
  armTurnTimer(hand);
  emitHandState(hand);
  return{ok:true,state:handPublicState(hand)};
}


async function processLeaveAfterHand(tableId){
  const snapshot=await tableSnapshot(tableId);
  const leaving=snapshot.seats.filter(s=>leaveAfterHandUsers.has(s.user_id));
  if(!leaving.length)return false;

  for(const seat of leaving){
    const {data:wallet,error:we}=await db.from('poker_wallets')
      .select('chips').eq('user_id',seat.user_id).single();
    if(we)throw we;

    const newBalance=Number(wallet.chips||0)+Number(seat.stack||0);

    const {error:uw}=await db.from('poker_wallets')
      .update({chips:newBalance}).eq('user_id',seat.user_id);
    if(uw)throw uw;

    const {error:del}=await db.from('poker_seats')
      .delete().eq('table_id',tableId).eq('user_id',seat.user_id);
    if(del)throw del;

    leaveAfterHandUsers.delete(seat.user_id);

    const socketId=userSockets.get(seat.user_id);
    if(socketId){
      io.to(socketId).emit('poker:table:left-after-hand',{wallet:newBalance});
      const s=io.sockets.sockets.get(socketId);
      if(s){
        s.leave(TABLE_ROOM(tableId));
        s.data.tableId=null;
      }
    }
  }

  const fresh=await tableSnapshot(tableId);
  io.to(TABLE_ROOM(tableId)).emit('poker:table:state',fresh);
  io.emit('poker:lobby:changed');
  return true;
}

function scheduleServerHand(tableId, delay = 700) {
  if (liveHands.has(tableId) || startTimers.has(tableId)) return;
  const timer = setTimeout(async () => {
    startTimers.delete(tableId);
    try {
      await startServerHand(tableId);
    } catch (error) {
      console.error('ATR Poker start hand:', error);
    }
  }, delay);
  startTimers.set(tableId, timer);
}

async function sendCurrentHandToSocket(socket, tableId) {
  const hand = liveHands.get(tableId);
  if (!hand) return;
  socket.emit('poker:hand:state', handPublicState(hand));
  const hole = hand.holeByUser.get(socket.data.user.id);
  if (hole) {
    socket.emit('poker:hand:private', {
      tableId,
      handNo: hand.handNo,
      hole
    });
  }
}


async function ensureWallet(userId) {
  // First try to read an existing wallet. Using upsert(...ignoreDuplicates:true)
  // followed by .single() can return zero rows on conflict, which causes
  // PostgREST error: "Cannot coerce the result to a single JSON object".
  const { data: existing, error: readError } = await db
    .from('poker_wallets')
    .select('chips,last_free_claim_at')
    .eq('user_id', userId)
    .maybeSingle();

  if (readError) throw readError;
  if (existing) return existing;

  const { data: created, error: createError } = await db
    .from('poker_wallets')
    .insert({ user_id: userId })
    .select('chips,last_free_claim_at')
    .single();

  if (createError) {
    // In case two requests created the wallet at the same time,
    // read it once more instead of failing the connection.
    const { data: retry, error: retryError } = await db
      .from('poker_wallets')
      .select('chips,last_free_claim_at')
      .eq('user_id', userId)
      .maybeSingle();

    if (retryError) throw retryError;
    if (retry) return retry;
    throw createError;
  }

  return created;
}

async function tableSnapshot(tableId) {
  const [{ data: table, error: te }, { data: seats, error: se }] = await Promise.all([
    db.from('poker_tables').select('*').eq('id', tableId).eq('enabled', true).single(),
    db.from('poker_seats')
      .select('table_id,seat_no,user_id,stack,joined_at')
      .eq('table_id', tableId).order('seat_no')
  ]);
  if (te) throw te;
  if (se) throw se;

  const rows = seats || [];
  const ids = [...new Set(rows.map(s => s.user_id).filter(Boolean))];
  let profiles = [];
  if (ids.length) {
    const { data, error } = await db.from('profiles')
      .select('id,username,avatar_url').in('id', ids);
    if (error) throw error;
    profiles = data || [];
  }

  const profileMap = new Map(profiles.map(p => [p.id, p]));
  return {
    table,
    seats: rows.map(s => ({
      ...s,
      username: profileMap.get(s.user_id)?.username || null,
      avatar_url: profileMap.get(s.user_id)?.avatar_url || null
    }))
  };
}

async function lobbySnapshot() {
  const { data: tables, error } = await db.from('poker_tables')
    .select('*').eq('enabled', true).order('bb');
  if (error) throw error;

  const { data: seats, error: seatError } = await db.from('poker_seats')
    .select('table_id');
  if (seatError) throw seatError;

  const counts = (seats || []).reduce((m, s) => {
    m[s.table_id] = (m[s.table_id] || 0) + 1;
    return m;
  }, {});

  return (tables || []).map(t => ({ ...t, occupied: counts[t.id] || 0 }));
}

async function authenticateSocket(socket, next) {
  try {
    const token = socket.handshake.auth?.accessToken;
    if (!token) return next(new Error('AUTH_REQUIRED'));

    const { data, error } = await authClient.auth.getUser(token);
    if (error || !data?.user) return next(new Error('INVALID_SESSION'));

    socket.data.user = data.user;
    await ensureWallet(data.user.id);
    next();
  } catch (err) {
    next(err);
  }
}

io.use(authenticateSocket);

io.on('connection', async socket => {
  const user = socket.data.user;
  userSockets.set(user.id, socket.id);

  socket.emit('poker:connected', { userId: user.id });

  socket.on('poker:lobby:get', async (_, ack = () => {}) => {
    try {
      const [tables, wallet] = await Promise.all([
        lobbySnapshot(),
        ensureWallet(user.id)
      ]);
      ack({ ok: true, tables, wallet });
    } catch (error) {
      ack({ ok: false, error: error.message });
    }
  });

  socket.on('poker:table:join', async (payload = {}, ack = () => {}) => {
    try {
      const tableId = String(payload.tableId || '');
      const buyin = Math.floor(Number(payload.buyin || 0));
      const { data: table, error: tableError } = await db
        .from('poker_tables').select('*').eq('id', tableId).eq('enabled', true).single();
      if (tableError) throw tableError;
      if (buyin < table.min_buyin || buyin > table.max_buyin) {
        return ack({ ok: false, error: 'INVALID_BUYIN' });
      }

      // One transaction would be ideal; Phase 2 moves this to a Postgres RPC.
      // For Phase 1 we serialize the user's own join path in-process.
      const { data: existing } = await db.from('poker_seats')
        .select('*').eq('user_id', user.id).maybeSingle();

      if (existing) {
        if (existing.table_id !== tableId) {
          return ack({ ok: false, error: 'ALREADY_SEATED' });
        }

        socket.join(TABLE_ROOM(tableId));
        socket.data.tableId = tableId;
        const wallet = await ensureWallet(user.id);
        const fresh = await tableSnapshot(tableId);
        ack({
          ok: true,
          resumed: true,
          seatNo: Number(existing.seat_no),
          wallet: Number(wallet.chips),
          state: fresh
        });
        await sendCurrentHandToSocket(socket, tableId);
        if (!liveHands.has(tableId) && fresh.seats.length >= 2) {
          scheduleServerHand(tableId,2500);
        }
        return;
      }

      if (liveHands.has(tableId)) {
        return ack({ ok: false, error: 'HAND_IN_PROGRESS' });
      }

      const wallet = await ensureWallet(user.id);
      if (Number(wallet.chips) < buyin) return ack({ ok: false, error: 'NOT_ENOUGH_CHIPS' });

      const snapshot = await tableSnapshot(tableId);
      const used = new Set(snapshot.seats.map(s => Number(s.seat_no)));
      let seatNo = null;
      for (let i = 0; i < table.max_seats; i++) {
        if (!used.has(i)) { seatNo = i; break; }
      }
      if (seatNo === null) return ack({ ok: false, error: 'TABLE_FULL' });

      const newBalance = Number(wallet.chips) - buyin;
      const { error: walletError } = await db.from('poker_wallets')
        .update({ chips: newBalance, updated_at: new Date().toISOString() })
        .eq('user_id', user.id);
      if (walletError) throw walletError;

      const { error: seatError } = await db.from('poker_seats').insert({
        table_id: tableId, seat_no: seatNo, user_id: user.id, stack: buyin
      });
      if (seatError) {
        await db.from('poker_wallets')
          .update({ chips: wallet.chips, updated_at: new Date().toISOString() })
          .eq('user_id', user.id);
        throw seatError;
      }

      socket.join(TABLE_ROOM(tableId));
      socket.data.tableId = tableId;

      const fresh = await tableSnapshot(tableId);
      io.to(TABLE_ROOM(tableId)).emit('poker:table:state', fresh);
      io.emit('poker:lobby:changed');
      ack({ ok: true, seatNo, wallet: newBalance, state: fresh });

      if (fresh.seats.length >= 2) {
        scheduleServerHand(tableId,2500);
      }
    } catch (error) {
      ack({ ok: false, error: error.message });
    }
  });

  socket.on('poker:table:rebuy', async (payload = {}, ack = () => {}) => {
    try {
      const tableId=socket.data.tableId;
      if(!tableId)return ack({ok:false,error:'NOT_SEATED'});
      const hand=liveHands.get(tableId);
      if(hand && !hand.completed)return ack({ok:false,error:'HAND_IN_PROGRESS'});

      const {data:seat,error:seatError}=await db.from('poker_seats')
        .select('*').eq('table_id',tableId).eq('user_id',user.id).maybeSingle();
      if(seatError)throw seatError;
      if(!seat)return ack({ok:false,error:'NOT_SEATED'});
      if(Number(seat.stack)>0)return ack({ok:false,error:'STACK_NOT_ZERO'});

      const {data:table,error:tableError}=await db.from('poker_tables')
        .select('*').eq('id',tableId).eq('enabled',true).single();
      if(tableError)throw tableError;

      const requested=Math.floor(Number(payload.buyin||0));
      const buyin=requested||Number(table.max_buyin);
      if(buyin<Number(table.min_buyin)||buyin>Number(table.max_buyin))
        return ack({ok:false,error:'INVALID_BUYIN'});

      const wallet=await ensureWallet(user.id);
      if(Number(wallet.chips)<buyin)
        return ack({ok:false,error:'NOT_ENOUGH_CHIPS',wallet:Number(wallet.chips)});

      const newBalance=Number(wallet.chips)-buyin;
      const {error:walletError}=await db.from('poker_wallets')
        .update({chips:newBalance,updated_at:new Date().toISOString()}).eq('user_id',user.id);
      if(walletError)throw walletError;

      const {error:stackError}=await db.from('poker_seats')
        .update({stack:buyin}).eq('table_id',tableId).eq('user_id',user.id).eq('stack',0);
      if(stackError){
        await db.from('poker_wallets')
          .update({chips:wallet.chips,updated_at:new Date().toISOString()}).eq('user_id',user.id);
        throw stackError;
      }

      const fresh=await tableSnapshot(tableId);
      io.to(TABLE_ROOM(tableId)).emit('poker:table:state',fresh);
      io.emit('poker:lobby:changed');
      ack({ok:true,buyin,wallet:newBalance,state:fresh});

      if(!liveHands.has(tableId) && fresh.seats.filter(s=>Number(s.stack)>0).length>=2)
        scheduleServerHand(tableId,400);
    }catch(error){
      console.error('ATR Poker rebuy:',error);
      ack({ok:false,error:error.message||'REBUY_FAILED'});
    }
  });

  socket.on('poker:table:leave-after-hand', async (payload = {}, ack = () => {}) => {
    const enabled=payload?.enabled!==false;
    const userId=socket.data.user.id;
    if(enabled)leaveAfterHandUsers.add(userId);
    else leaveAfterHandUsers.delete(userId);
    ack({ok:true,enabled});
  });

  socket.on('poker:table:leave', async (_, ack = () => {}) => {
    try {
      const currentTableId = socket.data.tableId;
      if (currentTableId && liveHands.has(currentTableId)) {
        // V13.3 has no player-action phase yet. Restore posted blinds when
        // somebody stands up so the deal test cannot burn play-money chips.
        await abortLiveHand(currentTableId, { restoreStacks: true });
      }

      const { data: seat, error } = await db.from('poker_seats')
        .select('*').eq('user_id', user.id).maybeSingle();
      if (error) throw error;
      if (!seat) return ack({ ok: true });

      const wallet = await ensureWallet(user.id);
      const newBalance = Number(wallet.chips) + Number(seat.stack);

      const { error: walletError } = await db.from('poker_wallets')
        .update({ chips: newBalance, updated_at: new Date().toISOString() })
        .eq('user_id', user.id);
      if (walletError) throw walletError;

      const { error: deleteError } = await db.from('poker_seats')
        .delete().eq('user_id', user.id);
      if (deleteError) throw deleteError;

      socket.leave(TABLE_ROOM(seat.table_id));
      socket.data.tableId = null;

      const fresh = await tableSnapshot(seat.table_id);
      io.to(TABLE_ROOM(seat.table_id)).emit('poker:table:state', fresh);
      io.emit('poker:lobby:changed');
      ack({ ok: true, wallet: newBalance });
    } catch (error) {
      ack({ ok: false, error: error.message });
    }
  });


  socket.on('poker:hand:action', async (payload = {}, ack = () => {}) => {
    try {
      const tableId=socket.data.tableId;if(!tableId)return ack({ok:false,error:'NOT_SEATED'});
      const hand=liveHands.get(tableId);
      if(!hand)return ack({ok:false,error:'NO_ACTIVE_HAND'});
      if(hand.completed)return ack({ok:false,error:'HAND_COMPLETE',state:handPublicState(hand)});
      ack(await doServerAction(hand,socket.data.user.id,payload));
    } catch(error){console.error('ATR Poker action:',error);ack({ok:false,error:error.message||'ACTION_FAILED'});}
  });

  socket.on('poker:hand:get', async (_, ack = () => {}) => {
    const tableId = socket.data.tableId;
    if (!tableId) return ack({ ok: false, error: 'NOT_SEATED' });
    const hand = liveHands.get(tableId);
    if (!hand) return ack({ ok: true, hand: null });

    const state=handPublicState(hand);
    ack({ ok: true, hand: state });
    socket.emit('poker:hand:state', state);

    if(!hand.completed){
      const hole = hand.holeByUser.get(socket.data.user.id);
      if(hole){
        socket.emit('poker:hand:private',{
          tableId,
          handNo:hand.handNo,
          hole
        });
      }
    }
  });

  socket.on('poker:chat:send', async (payload = {}, ack = () => {}) => {
    const tableId = socket.data.tableId;
    const text = String(payload.text || '').replace(/[<>]/g, '').trim().slice(0, 120);
    if (!tableId || !text) return ack({ ok: false, error: 'INVALID_MESSAGE' });

    io.to(TABLE_ROOM(tableId)).emit('poker:chat:message', {
      userId: user.id,
      username: user.user_metadata?.username || user.email?.split('@')[0] || 'Rider',
      text,
      at: Date.now()
    });
    ack({ ok: true });
  });

  socket.on('disconnect', () => {
    userSockets.delete(user.id);
    // Intentionally do NOT auto-cash-out on transient disconnect.
    // Reconnect/resume logic is added with the authoritative game engine.
  });
});

app.get('/health', (_, res) => res.json({ ok: true, service: 'atr-poker', phase: '1.6.5-3player-sidepot-beta' }));

server.listen(PORT, () => {
  console.log(`ATR Poker backend listening on :${PORT}`);
});
