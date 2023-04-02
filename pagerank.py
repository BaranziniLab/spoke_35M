import numpy as np
import scipy as sp
import scipy.sparse
import networkx as nx
import multiprocessing as mp
import sys
import time


GRAPH_PATH = sys.argv[1]
SPLIT = int(sys.argv[2])
NCORES = int(sys.argv[3])


def main():
    G = nx.read_gpickle(GRAPH_PATH)
    personalization = {"Disease:DOID:14330":1}
    # pagerank_scipy(G, personalization)
    start_time = time.time()
    pagerank_scipy_parallel(G, personalization=personalization, split=SPLIT, ncores=NCORES)
    print("Completed in {} hrs".format(round((time.time()-start_time)/(60*60),2)))


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

    global A, x, dangling_weights, p 

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
    split_column_indices = np.array_split(range(A.shape[-1]), split)
    p = mp.Pool(ncores)
    for _ in range(max_iter):
        xlast = x
        x_list = p.map(get_pagerank, split_column_indices)
        x = np.concatenate(x_list)
        del(x_list)
        # check convergence, l1 norm
        err = np.absolute(x - xlast).sum()
        if err < N * tol:
            return dict(zip(nodelist, map(float, x)))
    raise nx.PowerIterationFailedConvergence(max_iter)
    p.close()
    p.join()



def get_pagerank(split_indices):
    return alpha * (x @ A[:,split_indices] + sum(x[split_indices][is_dangling]) * dangling_weights[split_indices]) + (1 - alpha) * p[split_indices]

    

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

