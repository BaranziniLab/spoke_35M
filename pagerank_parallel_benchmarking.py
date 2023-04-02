import numpy as np
import scipy as sp
import scipy.sparse
import networkx as nx
import multiprocessing as mp
import sys
import pickle
import time
import joblib


GRAPH_PATH = sys.argv[1]
SAVE_PATH = sys.argv[2]
SPLIT = int(sys.argv[3])
NCORES = int(sys.argv[4])

node_list = ['Compound:inchikey:QPIJOYAYKONXJD-ZYLBBPIUSA-N', 'Compound:inchikey:HBYRDDGQNQMGCZ-QMMMGPOBSA-N', 'Compound:inchikey:WISJHPKKVVBBCE-QGZVFWFLSA-N', 'Compound:inchikey:POJGLMCWEBQVGJ-QFIPXVFZSA-N', 'Compound:inchikey:DXWQLTOXWVWMOH-UHFFFAOYSA-N', 'Compound:inchikey:BRKIAEBRDYHMFE-KKQCDPINSA-N', 'Compound:inchikey:UWVIZLOHFWAKKC-MNGKLMTLSA-N', 'Compound:inchikey:YNTUOLUMNOOQFL-UHFFFAOYSA-N', 'Compound:CHEBI:143130', 'Compound:inchikey:LILZBQJGXXRVSP-AWEZNQCLSA-N', 'Compound:inchikey:OYFJDPJIGCMKKI-LADGPHEKSA-N', 'Compound:inchikey:HEFYJBZEIVFECQ-LJAQVGFWSA-N', 'Compound:inchikey:IPDUBURODPGKPZ-DGVOKRSTSA-O', 'Compound:inchikey:SEUMVDPBYULKQH-UHFFFAOYSA-N', 'Compound:inchikey:LIOKNOIJMJKVCG-RDSVHMIISA-N', 'Compound:inchikey:PEFWQSMSSURTDP-UHFFFAOYSA-N', 'Compound:inchikey:OTXANOLOOUNVSR-UHFFFAOYSA-N', 'Compound:inchikey:OZQPALZNLAIXSQ-SJLPKXTDSA-N', 'Compound:inchikey:UVSIBVLSEPOIBQ-UHFFFAOYSA-N']


def main():
    start_time_read = time.time()
    with open(GRAPH_PATH, "rb") as f:
        G = pickle.load(f)
    print("Graph is loaded in {} min".format(round((time.time()-start_time_read)/(60),2)))
    completion_time_list = []
    for item in node_list:
        personalization = {item:1}
        start_time = time.time()
        pagerank_scipy_parallel(G, personalization=personalization, split=SPLIT, ncores=NCORES)
        completion_time_list.append(round((time.time()-start_time)/(60*60),2))
    joblib.dump(completion_time_list, SAVE_PATH)


def pagerank_scipy_parallel(
    G,
    alpha=0.85,
    personalization=None,
    max_iter=100,
    tol=1.0e-6,
    nstart=None,
    weight="weight",
    dangling=None,
    split=1,
    ncores=1    
):

    global A, x, dangling_weights, p, is_dangling

    N = len(G)
    if N == 0:
        return {}

    nodelist = list(G)
    A = nx.to_scipy_sparse_array(G, nodelist=nodelist, weight=weight, dtype=float)
    S = A.sum(axis=1)
    S = np.ravel(S)
    S[S != 0] = 1.0 / S[S != 0]
    # TODO: csr_array
    Q = sp.sparse.csr_matrix(sp.sparse.spdiags(S.T, 0, *A.shape))
    A = Q @ A

    # initial vector
    if nstart is None:
        x = np.repeat(1.0 / N, N)
    else:
        x = np.array([nstart.get(n, 0) for n in nodelist], dtype=float)
        x /= x.sum()

    # Personalization vector
    if personalization is None:
        p = np.repeat(1.0 / N, N)
    else:
        p = np.array([personalization.get(n, 0) for n in nodelist], dtype=float)
        if p.sum() == 0:
            raise ZeroDivisionError
        p /= p.sum()
    # Dangling nodes
    if dangling is None:
        dangling_weights = p
    else:
        # Convert the dangling dictionary into an array in nodelist order
        dangling_weights = np.array([dangling.get(n, 0) for n in nodelist], dtype=float)
        dangling_weights /= dangling_weights.sum()
    is_dangling = np.where(S == 0)[0]

    # power iteration: make up to max_iter iterations
    split_column_indices = np.array_split(range(A.shape[-1]), split)
    pagerank_args = list(zip([alpha]*len(split_column_indices), split_column_indices))
    p = mp.Pool(ncores)
    for _ in range(max_iter):
        xlast = x
        x_list = p.starmap(get_pagerank, pagerank_args)
        x = np.concatenate(x_list)
        del(x_list)
        # check convergence, l1 norm
        err = np.absolute(x - xlast).sum()
        if err < N * tol:
            return dict(zip(nodelist, map(float, x)))
    raise nx.PowerIterationFailedConvergence(max_iter)
    p.close()
    p.join()



def get_pagerank(alpha_, split_indices):
    return alpha_ * (x @ A[:,split_indices] + sum(x[split_indices][is_dangling]) * dangling_weights[split_indices]) + (1 - alpha_) * p[split_indices]

    

def pagerank_scipy(
    G,
    alpha=0.85,
    personalization=None,
    max_iter=100,
    tol=1.0e-6,
    nstart=None,
    weight="weight",
    dangling=None,
):


    N = len(G)
    if N == 0:
        return {}

    nodelist = list(G)
    A = nx.to_scipy_sparse_matrix(G, nodelist=nodelist, weight=weight, dtype=float)
    S = A.sum(axis=1)
    S = np.ravel(S)
    S[S != 0] = 1.0 / S[S != 0]
    # TODO: csr_array
    Q = sp.sparse.csr_matrix(sp.sparse.spdiags(S.T, 0, *A.shape))
    A = Q @ A

    # initial vector
    if nstart is None:
        x = np.repeat(1.0 / N, N)
    else:
        x = np.array([nstart.get(n, 0) for n in nodelist], dtype=float)
        x /= x.sum()

    # Personalization vector
    if personalization is None:
        p = np.repeat(1.0 / N, N)
    else:
        p = np.array([personalization.get(n, 0) for n in nodelist], dtype=float)
        if p.sum() == 0:
            raise ZeroDivisionError
        p /= p.sum()
    # Dangling nodes
    if dangling is None:
        dangling_weights = p
    else:
        # Convert the dangling dictionary into an array in nodelist order
        dangling_weights = np.array([dangling.get(n, 0) for n in nodelist], dtype=float)
        dangling_weights /= dangling_weights.sum()
    is_dangling = np.where(S == 0)[0]

    # power iteration: make up to max_iter iterations
    for _ in range(max_iter):
        xlast = x
        x = alpha * (x @ A + sum(x[is_dangling]) * dangling_weights) + (1 - alpha) * p
        # check convergence, l1 norm
        err = np.absolute(x - xlast).sum()
        if err < N * tol:
            return dict(zip(nodelist, map(float, x)))
    raise nx.PowerIterationFailedConvergence(max_iter)


if __name__ == "__main__":
    main()

